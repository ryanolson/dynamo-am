// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/// Extract the host portion from an endpoint URL.
///
/// For TCP endpoints (tcp://host:port), returns the host part.
/// For IPC endpoints (ipc://path), returns "localhost".
/// Returns None for unrecognized endpoint formats.
pub(crate) fn extract_host(endpoint: &str) -> Option<String> {
    if let Some(rest) = endpoint.strip_prefix("tcp://") {
        // Handle IPv6 addresses in [::1]:port format
        if let Some(stripped) = rest.strip_prefix('[') {
            let end = stripped.find(']')?;
            return Some(stripped[..end].to_string());
        }
        // Check for malformed IPv6 (contains ] without [)
        if rest.contains(']') {
            return None;
        }
        // Handle IPv4/hostname:port format
        let host = rest.split(':').next().map(|s| s.to_string())?;
        // Return None for empty host
        if host.is_empty() { None } else { Some(host) }
    } else if endpoint.starts_with("ipc://") {
        Some("localhost".to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host_tcp() {
        assert_eq!(
            extract_host("tcp://192.168.1.1:5555"),
            Some("192.168.1.1".to_string())
        );
        assert_eq!(
            extract_host("tcp://localhost:8080"),
            Some("localhost".to_string())
        );
    }

    #[test]
    fn test_extract_host_ipc() {
        assert_eq!(
            extract_host("ipc:///tmp/socket"),
            Some("localhost".to_string())
        );
    }

    #[test]
    fn test_extract_host_ipv6() {
        assert_eq!(extract_host("tcp://[::1]:5555"), Some("::1".to_string()));
        assert_eq!(
            extract_host("tcp://[2001:db8::1]:8080"),
            Some("2001:db8::1".to_string())
        );
        assert_eq!(
            extract_host("tcp://[::ffff:192.168.1.1]:9090"),
            Some("::ffff:192.168.1.1".to_string())
        );
    }

    #[test]
    fn test_extract_host_invalid() {
        assert_eq!(extract_host("invalid://endpoint"), None);
        assert_eq!(extract_host("http://example.com"), None);
        assert_eq!(extract_host(""), None);
        // Malformed IPv6
        assert_eq!(extract_host("tcp://[::1:5555"), None);
        assert_eq!(extract_host("tcp://::1]:5555"), None);
    }
}
