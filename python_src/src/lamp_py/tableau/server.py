import os
from typing import (
    List,
    Optional,
)

import tableauserverclient as TSC


def tableau_server(
    url: Optional[str] = os.getenv("TABLEAU_SERVER"),
) -> TSC.server.server.Server:
    """
    Get Tableau Server object

    :param url: URL of server, defaults to TABLEAU_SERVER env var

    :return TSC.Server object
    """
    return TSC.Server(server_address=url)


def tableau_authentication(
    tableau_user: Optional[str] = os.getenv("TABLEAU_USER"),
    tableau_password: Optional[str] = os.getenv("TABLEAU_PASSWORD"),
    tableau_site: str = "",  # empty string corresponds to Default site
) -> TSC.models.tableau_auth.TableauAuth:
    """
    Get Tableau Authentication object

    :param tablea_user: User to authenticate as, defaults to TABLEAU_USER env var
    :param tablea_password: Password to authenticate with, defaults to TABLEAU_PASSWORD evn var
    :param tablea_sute: Site to authenticate to, will default to 'Default' site

    :return TSC.TableaAuth  objects
    """
    return TSC.TableauAuth(
        username=tableau_user,
        password=tableau_password,
        site_id=tableau_site,
    )


def project_list(
    server: Optional[TSC.server.server.Server] = None,
    auth: Optional[TSC.models.tableau_auth.TableauAuth] = None,
) -> List[TSC.models.project_item.ProjectItem]:
    """
    Get List of all projects from Tablea server

    :param server: Tableau server to retrieve projects from
    :param auth:  Tableau authentication to use with server

    :return List[ProjectItem]
    """
    if server is None:
        server = tableau_server()
    if auth is None:
        auth = tableau_authentication()

    with server.auth.sign_in(auth):
        return list(TSC.Pager(server.projects))


def datasource_list(
    server: Optional[TSC.server.server.Server] = None,
    auth: Optional[TSC.models.tableau_auth.TableauAuth] = None,
) -> List[TSC.models.datasource_item.DatasourceItem]:
    """
    Get List of all datasources from Tablea server

    :param server: Tableau server to retrieve projects from
    :param auth:  Tableau authentication to use with server

    :return List[DatasourceItem]
    """
    if server is None:
        server = tableau_server()
    if auth is None:
        auth = tableau_authentication()

    with server.auth.sign_in(auth):
        return list(TSC.Pager(server.datasources))


def project_from_name(
    project_name: str,
    server: Optional[TSC.server.server.Server] = None,
    auth: Optional[TSC.models.tableau_auth.TableauAuth] = None,
) -> Optional[TSC.models.project_item.ProjectItem]:
    """
    Get Tableau ProjectItem from name
    """
    for project in project_list(server, auth):
        if project.name == project_name:
            return project

    return None


def datasource_from_name(
    datasource_name: str,
    server: Optional[TSC.server.server.Server] = None,
    auth: Optional[TSC.models.tableau_auth.TableauAuth] = None,
) -> Optional[TSC.models.datasource_item.DatasourceItem]:
    """
    Get Tableau DatasourceItem from name
    """
    if server is None:
        server = tableau_server()
    if auth is None:
        auth = tableau_authentication()

    for datasource in datasource_list(server, auth):
        if datasource.name == datasource_name:
            return datasource

    return None


def overwrite_datasource(
    project_name: str,
    hyper_path: str,
    server: Optional[TSC.server.server.Server] = None,
    auth: Optional[TSC.models.tableau_auth.TableauAuth] = None,
) -> TSC.models.datasource_item.DatasourceItem:
    """
    Publish locally saved hyperfile to Tableau
    """
    if server is None:
        server = tableau_server()
    if auth is None:
        auth = tableau_authentication()

    publish_mode = TSC.Server.PublishMode.Overwrite

    project = project_from_name(project_name)
    if project is None:
        raise KeyError(f"{project_name} project not found on Tableau server.")

    datasource = TSC.DatasourceItem(project_id=project.id)

    with server.auth.sign_in(auth):
        return server.datasources.publish(
            datasource_item=datasource,
            file=hyper_path,
            mode=publish_mode,
        )
