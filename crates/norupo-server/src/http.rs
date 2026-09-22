//! HTTP plumbing shared by the public ingress and the peer listener.

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use norupo_proto::Header;

/// The body type every handler in this crate returns.
pub type ResponseBody = BoxBody<Bytes, std::io::Error>;

/// Headers that describe a single hop and must never be forwarded.
///
/// Passing `Transfer-Encoding` or `Connection` through a proxy is the classic
/// request-smuggling primitive; passing `Upgrade` through breaks WebSockets in
/// confusing ways. See RFC 9110 §7.6.1.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Header marking a request that has already been handed between edge nodes.
/// Its presence stops two nodes bouncing a request back and forth forever.
pub const HOP_HEADER: &str = "x-norupo-hop";

/// Strips hop-by-hop headers, including any listed in `Connection`.
#[must_use]
pub fn sanitize_headers(headers: &HeaderMap) -> Vec<Header> {
    // `Connection: X, Y` nominates X and Y as hop-by-hop for this hop only.
    let mut connection_listed: Vec<String> = Vec::new();
    for value in headers.get_all(http::header::CONNECTION) {
        if let Ok(text) = value.to_str() {
            connection_listed.extend(text.split(',').map(|t| t.trim().to_ascii_lowercase()));
        }
    }

    headers
        .iter()
        .filter(|(name, _)| {
            let lower = name.as_str().to_ascii_lowercase();
            !HOP_BY_HOP.contains(&lower.as_str()) && !connection_listed.contains(&lower)
        })
        .map(|(name, value)| Header {
            name: name.as_str().to_string(),
            value: Bytes::copy_from_slice(value.as_bytes()),
        })
        .collect()
}

/// Rebuilds a [`HeaderMap`] from protocol headers, dropping anything malformed.
///
/// A misbehaving agent must not be able to inject a header that crashes the
/// edge or splits the response, so invalid names/values are dropped rather than
/// propagated.
#[must_use]
pub fn to_header_map(headers: &[Header]) -> HeaderMap {
    let mut map = HeaderMap::with_capacity(headers.len());
    for header in headers {
        let lower = header.name.to_ascii_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str()) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::try_from(header.name.as_str()),
            HeaderValue::from_bytes(&header.value),
        ) else {
            continue;
        };
        map.append(name, value);
    }
    map
}

/// Wraps bytes in the crate's boxed body type.
#[must_use]
pub fn full_body(bytes: impl Into<Bytes>) -> ResponseBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed()
}

/// Renders an edge-generated error page.
///
/// These are the pages a user sees when their tunnel is down, so they say what
/// happened and what to do about it rather than just a status code.
#[must_use]
pub fn error_page(status: StatusCode, title: &str, detail: &str) -> Response<ResponseBody> {
    let body = format!(
        "<!doctype html>\n<html><head><meta charset=\"utf-8\">\
<title>{code} {title}</title>\
<style>body{{font:16px/1.6 system-ui,sans-serif;max-width:40rem;margin:6rem auto;padding:0 1.5rem;color:#1a1a1a}}\
h1{{font-size:1.4rem;margin:0 0 .5rem}}code{{background:#f4f4f5;padding:.1rem .3rem;border-radius:.2rem}}\
footer{{margin-top:2rem;color:#71717a;font-size:.85rem}}</style></head>\
<body><h1>{code} &middot; {title}</h1><p>{detail}</p>\
<footer>norupo edge</footer></body></html>\n",
        code = status.as_u16(),
        title = html_escape(title),
        detail = detail, // callers pass pre-escaped or trusted markup
    );

    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(http::header::CACHE_CONTROL, "no-store")
        .body(full_body(body))
        .expect("static error response is always valid")
}

/// Minimal HTML escaping for values interpolated into error pages.
#[must_use]
pub fn html_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_by_hop_headers_are_stripped() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("api.tunnel.com"));
        headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        headers.insert(
            "connection",
            HeaderValue::from_static("keep-alive, x-custom"),
        );
        headers.insert(
            "x-custom",
            HeaderValue::from_static("nominated-by-connection"),
        );
        headers.insert("accept", HeaderValue::from_static("*/*"));

        let names: Vec<String> = sanitize_headers(&headers)
            .into_iter()
            .map(|h| h.name)
            .collect();

        assert!(names.contains(&"host".to_string()));
        assert!(names.contains(&"accept".to_string()));
        assert!(!names.contains(&"transfer-encoding".to_string()));
        assert!(!names.contains(&"connection".to_string()));
        // Nominated by `Connection`, so hop-by-hop for this hop.
        assert!(!names.contains(&"x-custom".to_string()));
    }

    #[test]
    fn malformed_agent_headers_are_dropped_not_propagated() {
        let map = to_header_map(&[
            Header {
                name: "x-good".into(),
                value: Bytes::from_static(b"1"),
            },
            // Newline in a value would split the response if it got through.
            Header {
                name: "x-bad".into(),
                value: Bytes::from_static(b"a\r\nInjected: yes"),
            },
            // Invalid header name.
            Header {
                name: "bad name".into(),
                value: Bytes::from_static(b"1"),
            },
            // Hop-by-hop from the agent is equally unwelcome.
            Header {
                name: "Transfer-Encoding".into(),
                value: Bytes::from_static(b"chunked"),
            },
        ]);

        assert_eq!(map.len(), 1);
        assert_eq!(map.get("x-good").unwrap(), "1");
        assert!(map.get("injected").is_none());
        assert!(map.get("transfer-encoding").is_none());
    }

    #[test]
    fn repeated_headers_survive_the_round_trip() {
        // Set-Cookie must not be collapsed; `append` is load-bearing.
        let map = to_header_map(&[
            Header {
                name: "set-cookie".into(),
                value: Bytes::from_static(b"a=1"),
            },
            Header {
                name: "set-cookie".into(),
                value: Bytes::from_static(b"b=2"),
            },
        ]);
        assert_eq!(map.get_all("set-cookie").iter().count(), 2);
    }

    #[test]
    fn error_pages_escape_interpolated_values() {
        let page = error_page(
            StatusCode::NOT_FOUND,
            "Tunnel not found",
            &html_escape("<script>alert(1)</script>"),
        );
        assert_eq!(page.status(), StatusCode::NOT_FOUND);
        // The escaping helper is what protects the host name we echo back.
        assert!(!html_escape("<script>").contains('<'));
    }
}
