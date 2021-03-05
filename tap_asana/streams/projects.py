import singer

from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream
from tap_asana.streams.base import invalid_token_handler
from oauthlib.oauth2 import TokenExpiredError


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
    try:
      for workspace in Context.asana.client.workspaces.find_all():
        for project in Context.asana.client.projects.find_all(workspace=workspace["gid"], opt_fields=opt_fields):
          session_bookmark = self.get_updated_session_bookmark(session_bookmark, project[self.replication_key])
          if self.is_bookmark_old(project[self.replication_key]):
            yield project
    except TokenExpiredError as TEE:
      LOGGER.info("ATTENTION: TokenExpiredError exception caught in Projects get_objects")
      invalid_token_handler(TEE)
    except Exception as e:
      LOGGER.info("ATTENTION: Generic exception caught in Projects get_objects")
      invalid_token_handler(e)

    self.update_bookmark(session_bookmark)


Context.stream_objects["projects"] = Projects
