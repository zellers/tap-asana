import singer

from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream
from tap_asana.streams.base import invalid_token_handler
from oauthlib.oauth2 import TokenExpiredError
import time



class Projects(Stream):
  name = "projects"
  replication_key = "modified_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "name",
    "gid",
    "owner",
    "current_status",
    "custom_fields",
    "default_view",
    "due_date",
    "due_on",
    "html_notes",
    "is_template",
    "created_at",
    "modified_at",
    "start_on",
    "archived",
    "public",
    "members",
    "followers",
    "color",
    "notes",
    "icon",
    "permalink_url",
    "workspace",
    "team"
  ]


  def get_objects(self):
    LOGGER = singer.get_logger()
    LOGGER.info("ATTENTION: Starting Projects Sync")
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    opt_fields = ",".join(self.fields)

    # Refreshing token at the start of Projects
    Context.asana.refresh_access_token()
    start_timer = time.time()

    for workspace in Context.asana.client.workspaces.find_all():
      for project in Context.asana.client.projects.find_all(workspace=workspace["gid"], opt_fields=opt_fields):

        if (time.time() - start_timer) > 1800:
          LOGGER.info("ATTENTION: 30 min passed in projects, refreshing token")
          Context.asana.refresh_access_token()
          start_timer = time.time()  # start timer over

        session_bookmark = self.get_updated_session_bookmark(session_bookmark, project[self.replication_key])
        if self.is_bookmark_old(project[self.replication_key]):
          yield project

    self.update_bookmark(session_bookmark)


Context.stream_objects["projects"] = Projects
