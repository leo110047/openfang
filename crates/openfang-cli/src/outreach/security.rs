use openfang_types::outreach::OutreachPlatformManifest;
use std::net::{IpAddr, ToSocketAddrs};
use url::Url;

pub(super) fn validate_platform_url(
    url: &str,
    manifest: &OutreachPlatformManifest,
) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|err| format!("source_url is invalid: {err}"))?;
    if parsed.scheme() != "https" {
        return Err("source_url must be an https URL".to_string());
    }
    if !manifest.is_allowed_url(url) {
        return Err(format!(
            "source URL is outside the {} source allowlist",
            manifest.display_name
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "source_url must include a host".to_string())?;
    reject_disallowed_addresses(host)?;
    Ok(())
}

fn reject_disallowed_addresses(host: &str) -> Result<(), String> {
    let addresses = format!("{host}:443")
        .to_socket_addrs()
        .map_err(|err| format!("source_url host could not be resolved: {err}"))?;
    for address in addresses {
        let ip = address.ip();
        if is_disallowed_ip(&ip) {
            return Err(format!(
                "source_url host resolves to a disallowed address: {ip}"
            ));
        }
    }
    Ok(())
}

fn is_disallowed_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.octets()[0] >= 240
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.segments()[0] & 0xfe00 == 0xfc00
                || ip.segments()[0] & 0xffc0 == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openfang_types::outreach::{
        BrowserProfileManifest, CostPolicy, DispatchSelectors, OutreachPlatformManifest,
        ReadStrategyManifest,
    };

    fn manifest_for_host(host: &str) -> OutreachPlatformManifest {
        OutreachPlatformManifest {
            key: "test".to_string(),
            display_name: "Test Platform".to_string(),
            sources: vec!["test".to_string()],
            allowed_hosts: vec![host.to_string()],
            allowed_path_prefixes: vec!["/cases/".to_string()],
            login_url: format!("https://{host}/login"),
            auth_check_path: "/dashboard".to_string(),
            profile: BrowserProfileManifest {
                key: "test".to_string(),
                persistent: true,
            },
            read_strategy: ReadStrategyManifest::default(),
            selectors: DispatchSelectors {
                message_env: "OPENFANG_TEST_MESSAGE_SELECTOR".to_string(),
                send_env: "OPENFANG_TEST_SEND_SELECTOR".to_string(),
                success_env: "OPENFANG_TEST_SUCCESS_SELECTOR".to_string(),
            },
            cost_policy: CostPolicy {
                required: true,
                kind: "credits".to_string(),
                limit: "single_contact".to_string(),
            },
        }
    }

    #[test]
    fn validate_platform_url_rejects_localhost_even_when_allowlisted() {
        let manifest = manifest_for_host("localhost");
        let err = validate_platform_url("https://localhost/cases/123", &manifest).unwrap_err();
        assert!(err.contains("disallowed address"));
    }

    #[test]
    fn validate_platform_url_rejects_non_default_https_port() {
        let manifest = manifest_for_host("www.example.com");
        let err =
            validate_platform_url("https://www.example.com:8443/cases/123", &manifest).unwrap_err();
        assert!(err.contains("allowlist"));
    }

    #[test]
    fn rejects_ipv6_link_local_addresses() {
        let ip: IpAddr = "fe80::1".parse().unwrap();
        assert!(is_disallowed_ip(&ip));
    }
}
