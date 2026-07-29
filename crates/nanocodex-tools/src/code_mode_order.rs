use nanocodex_oai_api::tools::ToolDefinition;

/// Matches Codex's Code Mode prompt order: plain tools first, followed by
/// namespaced tools ordered by namespace and member name.
pub(crate) fn sort_definitions(definitions: &mut [ToolDefinition]) {
    definitions.sort_by(|left, right| {
        let (left_namespace, left_name) = namespace_and_name(left.name())
            .map_or((None, left.name()), |(namespace, name)| {
                (Some(namespace), name)
            });
        let (right_namespace, right_name) = namespace_and_name(right.name())
            .map_or((None, right.name()), |(namespace, name)| {
                (Some(namespace), name)
            });
        left_namespace
            .cmp(&right_namespace)
            .then_with(|| left_name.cmp(right_name))
            .then_with(|| left.name().cmp(right.name()))
    });
}

fn namespace_and_name(name: &str) -> Option<(&str, &str)> {
    let (namespace, name) = name.split_once("__")?;
    (!namespace.is_empty() && !name.is_empty()).then_some((namespace, name))
}
