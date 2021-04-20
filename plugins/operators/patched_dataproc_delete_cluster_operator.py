from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator


class PatchedDataprocDeleteClusterOperator(DataprocDeleteClusterOperator):
    template_fields = ('impersonation_chain', 'cluster_name')