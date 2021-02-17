
from singer import utils
import singer
from oauthlib.oauth2 import TokenExpiredError
from tap_asana.streams.base import invalid_token_handler
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Tasks(Stream):
  name = 'tasks'
  replication_key = "modified_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "gid",
    "resource_type",
    "name",
    "approval_status",
    "assignee_status",
    "completed",
    "completed_at",
    "completed_by",
    "created_at",
    "dependencies",
    "dependents",
    "due_at",
    "due_on",
    "external",
    "hearted",
    "hearts",
    "html_notes",
    "is_rendered_as_seperator",
    "liked",
    "likes",
    "memberships",
    "modified_at",
    "notes",
    "num_hearts",
    "num_likes",
    "num_subtasks",
    "resource_subtype",
    "start_on",
    "assignee",
    "custom_fields",
    "followers",
    "parent",
    "permalink_url",
    "projects",
    "tags",
    "workspace"
  ]


  def get_objects(self):
    LOGGER = singer.get_logger()
    LOGGER.info("ATTENTION: Starting Tasks Sync")
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
    opt_fields = ",".join(self.fields)
    try:
      for workspace in self.call_api("workspaces"):
        for project in self.call_api("projects", workspace=workspace["gid"]):
          for task in self.call_api("tasks", project=project["gid"], opt_fields=opt_fields, modified_since=modified_since):
            session_bookmark = self.get_updated_session_bookmark(session_bookmark, task[self.replication_key])
            if self.is_bookmark_old(task[self.replication_key]):
              yield task
    except TokenExpiredError as TEE:
      LOGGER.info("ATTENTION: Exception Caught in tasks get_objects", TEE)
      invalid_token_handler()
    except Exception as e:
      LOGGER.info("ATTENTION: Generic exception caught in call_api", e)
      invalid_token_handler()
    self.update_bookmark(session_bookmark)


Context.stream_objects['tasks'] = Tasks
