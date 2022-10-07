import singer
import time
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Tags(Stream):
  name = 'tags'
  replication_key = "created_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "gid",
    "resource_type",
    "created_at",
    "followers",
    "name",
    "color",
    "notes",
    "permalink_url",
    "workspace"
  ]  


  def get_objects(self):
    LOGGER = singer.get_logger()
    LOGGER.info("ATTENTION: Starting Tags Sync")

    # Refreshing token at the start of Tags
    Context.asana.refresh_access_token()
    self.start_time = time.time()

    opt_fields = ",".join(self.fields)
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    for workspace in self.call_api("workspaces"):
      for tag in self.call_api("tags", workspace=workspace["gid"], opt_fields=opt_fields):
        self.timer_check()  # check if need to refresh token
        session_bookmark = self.get_updated_session_bookmark(session_bookmark, tag[self.replication_key])
        if self.is_bookmark_old(tag[self.replication_key]):
          yield tag
    self.update_bookmark(session_bookmark)

  def timer_check(self):
      LOGGER = singer.get_logger()
      if (time.time() - self.start_time) > 1800:
          LOGGER.info("ATTENTION: 30 min passed in tags, refreshing token")
          Context.asana.refresh_access_token()
          self.start_time = time.time()

Context.stream_objects['tags'] = Tags
