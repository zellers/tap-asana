import singer

from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Teams(Stream):
  name = 'teams'
  replication_method = 'FULL_TABLE'
  fields = [
    "gid",
    "resource_type",
    "name",
    "description",
    "html_description",
    "organization",
    "permalink_url"
  ]


  def get_objects(self):
    LOGGER = singer.get_logger()
    LOGGER.info("ATTENTION: Starting Teams Sync")
    # Refreshing token at the start of Teams
    Context.asana.refresh_access_token()
    opt_fields = ",".join(self.fields)
    for workspace in Context.asana.client.workspaces.find_all(opt_fields="gid,is_organization"):
      if workspace.get('is_organization', False) == True:
        for team in Context.asana.client.teams.find_by_organization(organization=workspace["gid"], opt_fields=opt_fields):
          users = []
          for user in Context.asana.client.teams.users(team=team["gid"]):
            users.append(user)
          team['users'] = users
          yield team


Context.stream_objects['teams'] = Teams
