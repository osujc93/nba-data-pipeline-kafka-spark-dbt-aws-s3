"""
Helper classes for the NBA DAG.

DockerOperatorWithCleanup: Subclass of DockerOperator that forcibly removes
    any existing Docker container with the same container_name.
BranchDecider: Provides a method to decide whether to run initial or
    incremental ingestion based on an Airflow Variable.
"""

import docker
from typing import Any, Dict

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable


class DockerOperatorWithCleanup(DockerOperator):
    """
    Subclass of DockerOperator that removes any existing container before creation.

    Ensures we don't get a 409 Conflict if a container with the same name is still
    running or present from a previous run.
    """

    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Remove existing container if necessary, then execute the DockerOperator.

        :param context: The Airflow context dictionary.
        :return: The result of super().execute(context).
        """
        if self.container_name:
            self._remove_existing_container(self.container_name)
        return super().execute(context)

    def _remove_existing_container(self, container_name: str) -> None:
        """
        Remove any existing Docker container with the specified name if it exists.

        :param container_name: Name of the container to remove.
        """
        client = docker.from_env()
        try:
            container = client.containers.get(container_name)
            container.remove(force=True)
            print(f"Removed existing container: {container_name}")
        except docker.errors.NotFound:
            print(f"No existing container found with name: {container_name}")


class BranchDecider:
    """
    Encapsulates the branching logic for deciding whether to perform
    initial ingestion or skip to incremental ingestion.
    """

    @staticmethod
    def decide_initial_or_incremental() -> str:
        """
        Decide whether to do one-time ingestion or skip to incremental flow.

        Returns the task_id that the DAG should proceed with:
        - "skip_initial_chain" if 'init_ingestion_done' Airflow Variable is True
        - "do_initial_chain" otherwise

        :return: Task ID string to branch to.
        """
        flag = Variable.get("init_ingestion_done", default_var="False")
        if flag.lower() == "true":
            return "skip_initial_chain"
        return "do_initial_chain"
