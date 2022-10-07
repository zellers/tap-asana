import singer
import time
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream
from tap_asana.streams.base import invalid_token_handler
from oauthlib.oauth2 import TokenExpiredError


class Sections(Stream):
    name = 'sections'
    replication_method = 'FULL_TABLE'

    fields = [
        "gid",
        "resource_type",
        "name",
        "created_at",
        "project",
        "projects"
    ]

    def get_objects(self):
        LOGGER = singer.get_logger()
        LOGGER.info("ATTENTION: Starting Sections Sync")
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
        opt_fields = ",".join(self.fields)

        # Refreshing token at the start of sections
        Context.asana.refresh_access_token()
        start_timer = time.time()

        for workspace in self.call_api("workspaces"):
            for project in self.call_api("projects", workspace=workspace["gid"]):
                if (time.time() - start_timer) > 1800:
                    LOGGER.info("ATTENTION: 30 min passed in sections, refreshing token")
                    Context.asana.refresh_access_token()
                    start_timer = time.time()  # start timer over
                for section in Context.asana.client.sections.get_sections_for_project(project_gid=project["gid"],
                                                                                      owner="me",
                                                                                      opt_fields=opt_fields):
                    yield section


Context.stream_objects['sections'] = Sections
