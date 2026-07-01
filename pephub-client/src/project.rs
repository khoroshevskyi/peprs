use peprs_core::project::Project;

pub trait PEPHubClient {
    fn pull(&self) -> Project;

    fn push(
        &self,
        project: Project,
        namespace: &str,
        name: &str,
        tag: Option<&str>,
        private: Option<bool>,
        force: Option<bool>,
    );

    fn delete(&self, namespace: &str, name: &str, tag: Option<&str>);
}
