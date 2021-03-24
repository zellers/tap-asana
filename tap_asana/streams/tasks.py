from singer import utils
import singer
from tap_asana.streams.base import invalid_token_handler
from tap_asana.context import Context
from tap_asana.streams.base import Stream
from oauthlib.oauth2 import TokenExpiredError
import time


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

        # Refreshing token at the start of tasks
        Context.asana.refresh_access_token()
        start_timer = time.time()

        # Put all project ids into a list
        workspaces = self.call_api("workspaces")
        all_projects_gid = []
        for workspace in workspaces:
            projects = self.call_api("projects", workspace=workspace["gid"])
            for project in projects:
                all_projects_gid.append(project["gid"])

        for p_gid in all_projects_gid:
            # LOGGER.info("Next Project")
            # LOGGER.info(p_gid)
            tasks = self.call_api("tasks", project=p_gid, opt_fields=opt_fields,
                                  modified_since=modified_since)

            # LOGGER.info(tasks)
            # counter = 0
            # for task in tasks:
            #     LOGGER.info(task["gid"])
            #     counter += 1
            # if counter == 0:
            #     continue
            #
            # all_tasks_list_ids = []
            # self.get_all_tasks(tasks, all_tasks_list_ids)
            #
            # LOGGER.info(all_tasks_list_ids)
            #
            # for task_id in all_tasks_list_ids:
            #     task = Context.asana.client.tasks.find_by_id(task_id)
            #     yield task
            #     LOGGER.info("Yielded Task")
            task_list = []
            for task in tasks:
                if (time.time() - start_timer) > 1800:
                    LOGGER.info("ATTENTION: 30 min passed, refreshing token")
                    Context.asana.refresh_access_token()
                    start_timer = time.time()  # start timer over
                task_list.append(task["gid"])
                session_bookmark = self.get_updated_session_bookmark(session_bookmark, task[self.replication_key])
                if self.is_bookmark_old(task[self.replication_key]):
                    yield task
            LOGGER.info(task_list)

        self.update_bookmark(session_bookmark)

    def get_all_tasks(self, tasks, all_tasks_list_ids):

        for task in tasks:
            session_bookmark = self.get_updated_session_bookmark(session_bookmark, task[self.replication_key])
            if self.is_bookmark_old(task[self.replication_key]):
                all_tasks_list_ids.append(task["gid"])
            subtasks = Context.asana.client.tasks.subtasks(task["gid"])
            if len(list(subtasks)) == 0:
                continue
            else:
                self.get_all_tasks(subtasks, all_tasks_list_ids)


Context.stream_objects['tasks'] = Tasks
