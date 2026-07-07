pub(crate) fn join_endpoint_path(endpoint: &str, api_path: &str) -> String {
    format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        api_path.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::join_endpoint_path;

    #[test]
    fn join_endpoint_path_normalizes_slashes() {
        assert_eq!(
            join_endpoint_path("http://127.0.0.1:9428/", "/insert/jsonline"),
            "http://127.0.0.1:9428/insert/jsonline"
        );
        assert_eq!(
            join_endpoint_path("http://127.0.0.1:9428", "insert/jsonline"),
            "http://127.0.0.1:9428/insert/jsonline"
        );
    }
}
