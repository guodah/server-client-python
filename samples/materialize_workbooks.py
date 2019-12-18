import argparse
import getpass
import logging
import os
from prettytable import PrettyTable

from dateutil import tz

import tableauserverclient as TSC
from collections import defaultdict
from datetime import time

from tableauserverclient import TaskItem, ServerResponseError


def main():
    parser = argparse.ArgumentParser(description='Materialized Views settings for sites/workbooks.')
    parser.add_argument('--server', '-s', required=True, help='Tableau server address')
    parser.add_argument('--username', '-u', required=True, help='username to sign into server')
    parser.add_argument('--password', '-p', required=False, help='password to sign into server')
    parser.add_argument('--mode', '-m', required=False,
                        choices=['disable', 'enable', 'enable_all', 'enable_selective'],
                        help='update mode to Materialized Views settings for sites/workbooks')
    parser.add_argument('--status', '-st', required=False, action='store_const', const=True,
                        help='show Materialized Views enabled sites/workbooks')
    parser.add_argument('--site-url', '-si', required=False,
                        help='the server Default site will be use unless the site url or site name is specified')
    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')
    parser.add_argument('--type', '-t', required=False, default='workbook',
                        choices=['site', 'workbook', 'project-name', 'project-path'],
                        help='type of content you want to update or see Materialized Views settings on')
    parser.add_argument('--path-list', '-pl', required=False, help='path to a list of workbook paths')
    parser.add_argument('--workbook-path', '-wp', required=False, help='a workbook path (project/workbook)')
    parser.add_argument('--name-list', '-nl', required=False, help='path to a list of workbook names')
    parser.add_argument('--project-name', '-pn', required=False, help='name of the project')
    parser.add_argument('--project-path', '-pp', required=False, help="path of the project")
    parser.add_argument('--materialize-now', '-mn', required=False, action='store_true',
                        help='create Materialized Views for workbooks immediately')
    parser.add_argument('--create-schedule', '-cs', required=False,
                        help='create Materialized Views schedule')
    parser.add_argument('--show-schedules', '-ss', required=False, action='store_const', const=True,
                        help='show Materialized Views schedules')
    parser.add_argument('--remove-from-schedule', '-rfs', required=False, action='store_const', const=True,
                        help='remove workbooks from a Materialized Views schedule')
    parser.add_argument('--add-to-schedule', '-ats', required=False,
                        help='add workbooks to a Materialized Views schedule')
    parser.add_argument('--hourly-interval', '-hi', choices=['0.25', '0.5', '2', '1', '4', '6', '8', '12'],
                        required=False, help='schedule interval in hours')
    parser.add_argument('--weekly-interval', '-wi',
                        choices=['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
                        nargs="+", required=False, help='schedule interval in hours')
    parser.add_argument('--daily-interval', '-di', action='store_const', const=True,
                        required=False, help='schedule interval in hours')
    parser.add_argument('--monthly-interval', '-mi',
                        required=False, help='schedule interval in hours')
    parser.add_argument('--start-hour', '-sh', required=False, help='start time hour', default=0)
    parser.add_argument('--start-minute', '-sm', required=False,
                        choices=['0', '15', '30', '45'], help='start time minute', default=0)
    parser.add_argument('--end-hour', '-eh', required=False, help='end time hour', default=0)
    parser.add_argument('--end-minute', '-em', required=False, help='end time minute', default=0)

    args = parser.parse_args()

    if args.password:
        password = args.password
    else:
        password = getpass.getpass("Password: ")

    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level)

    # site content url is the TSC term for site id
    site_content_url = args.site_url if args.site_url is not None else ""

    materialized_views_config = create_materialized_views_config(args)

    if args.show_schedules is not None or args.create_schedule is not None or \
            args.remove_from_schedule is not None or args.add_to_schedule is not None:
        if not handle_schedule_command(args, password, site_content_url):
            return

    # enable/disable materialized views for site
    elif args.type == 'site':
        if not update_site(args, password, site_content_url):
            return

    # enable/disable materialized views for workbook
    # works only when the site the workbooks belong to are enabled too
    elif args.type == 'workbook' and args.mode is not None:
        if not update_workbook(args, materialized_views_config, password, site_content_url):
            return

    # enable/disable materialized views for project by project name
    # will show possible projects when project name is not unique
    elif args.type == 'project-name':
        if not update_project_by_name(args, materialized_views_config, password, site_content_url):
            return

    # enable/disable materialized views for project by project path, for example: project1/project2
    elif args.type == 'project-path':
        if not update_project_by_path(args, materialized_views_config, password, site_content_url):
            return

    # show enabled sites and workbooks
    if args.status:
        show_materialized_views_status(args, password, site_content_url)


def left_align_table(table):
    for field_name in table.field_names:
        table.align[field_name] = 'l'


def print_materialized_views_tasks(server, tasks, workbook_id_to_workbook=None):
    local_tz = tz.tzlocal()

    table = init_table(['Workbook', 'Schedule', 'Last Updated At', 'Next Run At'])

    header = "\nMaterialized Views Scheduled Tasks:"
    num_tasks = 0
    for task in tasks:
        workbook = server.workbooks.get_by_id(task.target.id)
        if workbook is not None and \
                (workbook_id_to_workbook is None or workbook.id in workbook_id_to_workbook):
            num_tasks += 1
            table.add_row(['{}/{}'.format(workbook.project_name, workbook.name),
                           task.schedule_item.name,
                           task.last_run_at.astimezone(local_tz) if task.last_run_at is not None else None,
                           task.schedule_item.next_run_at.astimezone(local_tz)
                           if task.schedule_item.next_run_at is not None else None])
    if num_tasks == 0:
        print("{} None".format(header))
    else:
        print(header)
        print(table)


def get_workbooks_from_paths(server, args):
    all_projects = {project.id: project for project in TSC.Pager(server.projects)}
    workbook_id_to_workbook = dict()
    workbook_path_mapping = parse_workbook_path(args.path_list)
    for workbook_name, workbook_paths in workbook_path_mapping.items():
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         workbook_name))
        workbooks = list(TSC.Pager(server.workbooks, req_option))
        all_paths = set(workbook_paths[:])
        for workbook in workbooks:
            path = find_project_path(all_projects[workbook.project_id], all_projects, "")
            if path in workbook_paths:
                all_paths.remove(path)
                workbook_id_to_workbook[workbook.id] = workbook

        for path in all_paths:
            print("Cannot find workbook path: {}, each line should only contain one workbook path"
                  .format(path + '/' + workbook_name))
    return workbook_id_to_workbook


def get_workbooks_from_names(server, args):
    workbook_id_to_workbook = dict()
    workbook_names = sanitize_workbook_list(args.name_list, "name")
    for workbook_name in workbook_names:
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         workbook_name.rstrip()))
        workbooks = list(TSC.Pager(server.workbooks, req_option))
        if len(workbooks) == 0:
            print("Cannot find workbook name: {}, each line should only contain one workbook name"
                  .format(workbook_name))
        for workbook in workbooks:
            workbook_id_to_workbook[workbook.id] = workbook
    return workbook_id_to_workbook


def get_workbook_from_path(server, args):
    all_projects = {project.id: project for project in TSC.Pager(server.projects)}
    workbook_id_to_workbook = dict()
    workbook_project = args.workbook_path.rstrip().split('/')
    workbook_path = '/'.join(workbook_project[:-1])
    workbook_name = workbook_project[-1]

    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     workbook_name))
    workbooks = list(TSC.Pager(server.workbooks, req_option))
    for workbook in workbooks:
        path = find_project_path(all_projects[workbook.project_id], all_projects, "")
        if path == workbook_path:
            workbook_id_to_workbook[workbook.id] = workbook
            break

    return workbook_id_to_workbook


def show_materialized_views_schedules(args, password, site_content_url):
    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        tasks = list(TSC.Pager(lambda options: server.tasks.get(task_type="materializeViews")))
        workbook_id_to_workbook = None
        if args.path_list is not None:
            workbook_id_to_workbook = get_workbooks_from_paths(server, args)
        elif args.name_list is not None:
            workbook_id_to_workbook = get_workbooks_from_names(server, args)
        elif args.workbook_path is not None:
            workbook_id_to_workbook = get_workbook_from_path(server, args)
        print_materialized_views_tasks(server, tasks, workbook_id_to_workbook)
    return True


def remove_materialized_views_tasks(server, tasks, workbook_id_to_workbook):
    if workbook_id_to_workbook is None or len(workbook_id_to_workbook) == 0:
        print("Unable to find any workbooks to remove schedule for")
        return False

    if tasks is None or len(tasks) == 0:
        print("Unable to find any MaterializeViews tasks")
        return False

    table = init_table(['Workbook', 'Removed From Schedule'])

    num_removed = 0
    for task in tasks:
        if task.target.id in workbook_id_to_workbook:
            try:
                server.tasks.delete(task.id, task_type="materializeViews")
                num_removed += 1
                table.add_row(['{}/{}'.format(workbook_id_to_workbook[task.target.id].project_name,
                                              workbook_id_to_workbook[task.target.id].name), task.schedule_item.name])
            except ServerResponseError as error:
                print("{}: {}".format(error.summary, error.detail))

    if num_removed > 0:
        print(table)
    else:
        print("Unable to find any workbooks to remove MaterializeViews for")


def remove_workbook_from_materialized_views(args, password, site_content_url):
    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        tasks = list(TSC.Pager(lambda options: server.tasks.get(task_type="materializeViews")))
        workbook_id_to_workbook = None
        if args.path_list is not None:
            workbook_id_to_workbook = get_workbooks_from_paths(server, args)
        elif args.name_list is not None:
            workbook_id_to_workbook = get_workbooks_from_names(server, args)
        remove_materialized_views_tasks(server, tasks, workbook_id_to_workbook, args.remove_from_schedule)
    return True


def find_schedule(server, schedule_name):
    if schedule_name is None:
        return None

    schedules = list(TSC.Pager(server.schedules.get))
    for schedule in schedules:
        if schedule_name == schedule.name:
            return schedule
    return None


def confirm(message):
    """
    Ask user to enter Y or N (case-insensitive).
    :return: True if the answer is Y.
    :rtype: bool
    """
    answer = ""
    while answer not in ["y", "n"]:
        answer = input(message).lower()
    return answer == "y"


def add_to_materialized_views_schedule(server, tasks, schedule, workbook_id_to_workbook):
    if schedule is None or workbook_id_to_workbook is None:
        return

    workbook_id_to_task = dict()
    if tasks is not None:
        for task in tasks:
            if task.target.type == 'workbook':
                workbook_id_to_task[task.target.id] = task

    table = PrettyTable()
    table.field_names = ['Project/Workbook', 'Added to or Remain on Schedule']
    left_align_table(table)

    num_added = 0
    for workbook in workbook_id_to_workbook.values():
        try:
            if workbook.id in workbook_id_to_task:
                task = workbook_id_to_task[workbook.id]
                print('Workbook \"{0}/{1}\" is already on schedule \"{2}\"'.format(
                    workbook.project_name, workbook.name, task.schedule_item.name))
                if task.schedule_item.id == schedule.id:
                    table.add_row(['{}/{}'.format(workbook.project_name, workbook.name), schedule.name])
                    continue
                if not confirm('Do you want to replace it with {} [Y/N]? '.format(schedule.name)):
                    table.add_row(['{}/{}'.format(workbook.project_name, workbook.name), task.schedule_item.name])
                    continue
                server.tasks.delete(workbook_id_to_task[workbook.id].id, TaskItem.Type.MaterializeViews)

            server.schedules.add_to_schedule(schedule.id, workbook, task_type="materializeViews")
            num_added += 1
            table.add_row(['{}/{}'.format(workbook.project_name, workbook.name), schedule.name])
        except ServerResponseError as error:
            print("{}: {}".format(error.summary, error.detail))

    if num_added > 0:
        print(table)
    print("\n")


def add_workbooks_to_schedule(args, password, site_content_url):
    schedule_name = args.add_to_schedule

    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        schedule = find_schedule(server, schedule_name)
        if schedule is None:
            print('Did not find the schedule "{}"'.format(schedule_name))
            return False

        tasks = list(TSC.Pager(lambda options: server.tasks.get(task_type="materializeViews")))

        workbook_id_to_workbook = None
        if args.path_list is not None:
            workbook_id_to_workbook = get_workbooks_from_paths(server, args)
        elif args.name_list is not None:
            workbook_id_to_workbook = get_workbooks_from_names(server, args)
        elif args.workbook_path is not None:
            workbook_id_to_workbook = get_workbook_from_path(server, args)
        add_to_materialized_views_schedule(server, tasks, schedule, workbook_id_to_workbook)
    return True


def verify_time_arguments(args):
    def schedule_type_none(schedule_type):
        if schedule_type is not None:
            print('Please select one of the schedule types: hourly-interval, daily-interval, '
                  'weekly-interval, monthly-interval')
            return False
        else:
            return True

    # verify start_time
    if args.start_hour is None or not (0 <= float(args.start_hour) <= 23) or \
            args.start_minute is None or not (0 <= float(args.start_minute) <= 59):
        print("Please provide the schedule start time. Ex: --start-hour 8 --start-minute 30")
        return False

    schedule_type_selected = None
    if args.daily_interval is not None:
        schedule_type_selected = "daily-interval"

    if args.weekly_interval is not None:
        if schedule_type_none(schedule_type_selected):
            schedule_type_selected = "weekly-interval"
        else:
            return False

    if args.monthly_interval is not None:
        if schedule_type_none(schedule_type_selected):
            if not (1 <= int(args.monthly_interval) <= 31):
                print('Please provide the monthly schedule between 1 and 31')
                return False
            schedule_type_selected = "monthly-interval"
        else:
            return False

    if args.hourly_interval is not None:
        if schedule_type_none(schedule_type_selected):
            if args.end_hour is None or not (0 <= args.end_hour <= 23) or \
                    args.end_minute is None or not (0 <= args.end_minute <= 59):
                print("Please provide the schedule end time. Ex: --end-hour 23 --end-minute 30")
                return False
            else:
                schedule_type_selected = 'hourly-schedule'
        else:
            return False

    return schedule_type_selected is not None


def get_hour_interval(hour_interval):
    if hour_interval in ['0.25', '0.5']:
        return float(hour_interval)
    else:
        return int(hour_interval)


def create_hourly_schedule(server, args):
    hourly_interval = TSC.HourlyInterval(start_time=time(int(args.start_hour), int(args.start_minute)),
                                         end_time=time(int(args.end_hour), int(args.end_minute)),
                                         interval_value=get_hour_interval(args.hourly_interval))

    schedule_name = args.create_schedule

    hourly_schedule = TSC.ScheduleItem(schedule_name, 75, TSC.ScheduleItem.Type.MaterializeViews,
                                       TSC.ScheduleItem.ExecutionOrder.Parallel, hourly_interval)
    hourly_schedule = server.schedules.create(hourly_schedule)
    if hourly_schedule is not None:
        print("Hourly schedule \"{}\" created with an interval of {} hours.".format(
            schedule_name, args.hourly_interval))
    else:
        print("Schedule {} was not created successfully".format(schedule_name))


def create_daily_schedule(server, args):
    daily_interval = TSC.DailyInterval(start_time=time(int(args.start_hour), int(args.start_minute)))

    schedule_name = args.create_schedule

    daily_schedule = TSC.ScheduleItem(schedule_name, 75, TSC.ScheduleItem.Type.MaterializeViews,
                                      TSC.ScheduleItem.ExecutionOrder.Parallel, daily_interval)
    daily_schedule = server.schedules.create(daily_schedule)
    if daily_schedule is not None:
        print("Daily schedule \"{}\" created to run at {:02d}:{:02d}.".format(
            schedule_name, int(args.start_hour), int(args.start_minute)))
    else:
        print("Schedule {} was not created successfully".format(schedule_name))


def create_weekly_schedule(server, args):
    weekly_interval = TSC.WeeklyInterval(time(int(args.start_hour), int(args.start_minute)),
                                         *args.weekly_interval)

    schedule_name = args.create_schedule

    weekly_schedule = TSC.ScheduleItem(schedule_name, 75, TSC.ScheduleItem.Type.MaterializeViews,
                                       TSC.ScheduleItem.ExecutionOrder.Parallel, weekly_interval)
    weekly_schedule = server.schedules.create(weekly_schedule)
    if weekly_schedule is not None:
        print("Weekly schedule \"{}\" created to run on {} at  {:02d}:{:02d}.".format(
            schedule_name, args.weekly_interval, int(args.start_hour), int(args.start_minute)))
    else:
        print("Schedule {} was not created successfully".format(schedule_name))


def create_monthly_schedule(server, args):
    monthly_interval = TSC.MonthlyInterval(start_time=time(int(args.start_hour), int(args.start_minute)),
                                           interval_value=args.monthly_interval)

    schedule_name = args.create_schedule

    monthly_schedule = TSC.ScheduleItem(schedule_name, 75, TSC.ScheduleItem.Type.MaterializeViews,
                                        TSC.ScheduleItem.ExecutionOrder.Parallel, monthly_interval)
    monthly_schedule = server.schedules.create(monthly_schedule)
    if monthly_schedule is not None:
        print("Monthly schedule \"{}\" created to run on {}th at {:02d}:{:02d}.".format(
            schedule_name, args.monthly_interval, int(args.start_hour), int(args.start_minute)))
    else:
        print("Schedule {} was not created successfully".format(schedule_name))


def create_materialized_view_schedule(args, password, site_content_url):
    # verifies start and end times
    if not verify_time_arguments(args):
        return False

    try:
        tableau_auth = TSC.TableauAuth(args.username, password)
        server = TSC.Server(args.server)
        with server.auth.sign_in(tableau_auth):
            if args.hourly_interval is not None:
                create_hourly_schedule(server, args)
            elif args.daily_interval is not None:
                create_daily_schedule(server, args)
            elif args.weekly_interval is not None:
                create_weekly_schedule(server, args)
            else:
                create_monthly_schedule(server, args)
    except ServerResponseError as error:
        print("{}: {}".format(error.summary, error.detail))
        return False

    return True


def handle_schedule_command(args, password, site_content_url):
    assert_schedule_action_options_valid(args)

    if args.show_schedules is not None:
        return show_materialized_views_schedules(args, password, site_content_url)
    elif args.remove_from_schedule is not None:
        return remove_workbook_from_materialized_views(args, password, site_content_url)
    elif args.add_to_schedule is not None:
        return add_workbooks_to_schedule(args, password, site_content_url)
    elif args.create_schedule:
        return create_materialized_view_schedule(args, password, site_content_url)
    else:
        print('Schedule option unrecognized. Accepted schedule options: create|add|delete|show')
        return False


def find_project_path(project, all_projects, path):
    # project stores the id of it's parent
    # this method is to run recursively to find the path from root project to given project
    path = project.name if len(path) == 0 else project.name + '/' + path

    if project.parent_id is None:
        return path
    else:
        return find_project_path(all_projects[project.parent_id], all_projects, path)


def get_project_paths(server, projects):
    # most likely user won't have too many projects so we store them in a dict to search
    all_projects = {project.id: project for project in TSC.Pager(server.projects)}

    result = dict()
    for project in projects:
        result[find_project_path(project, all_projects, "")] = project
    return result


def print_paths(paths):
    for path in paths.keys():
        print(path)


def init_table(header):
    table = PrettyTable()
    table.field_names = header
    left_align_table(table)
    return table


def get_and_print_materialized_views_enabled_sites(server):
    table = init_table(["Sites Enabled For Materialized Views"])

    enabled_sites = set()
    # For server admin, this will prints all the materialized views enabled sites
    # For other users, this only prints the status of the site they belong to
    # only server admins can get all the sites in the server
    # other users can only get the site they are in
    for site in TSC.Pager(server.sites):
        if site.materialized_views_mode != "disable":
            enabled_sites.add(site)
            table.add_row([site.name])

    header = "\nMaterialized Views is enabled on sites:"
    if len(enabled_sites) == 0:
        print("{} None".format(header))
    else:
        print(header)
        print(table)
    return enabled_sites


def print_materialized_views_enabled_workbooks(site_name_to_workbooks):
    header = "\nMaterialized Views is enabled on workbooks:"

    if len(site_name_to_workbooks) == 0:
        print("{} None".format(header))
    else:
        table = init_table(["Site", "Project/Workbook"])
        left_align_table(table)

        num_workbooks = 0
        for site_name in site_name_to_workbooks:
            workbooks = site_name_to_workbooks[site_name]
            for workbook in workbooks:
                if workbook.materialized_views_config['materialized_views_enabled']:
                    num_workbooks += len(workbooks)
                    table.add_row([site_name, '{}/{}'.format(workbook.project_name, workbook.name)])
        if num_workbooks == 0:
            print("{} None".format(header))
        else:
            print(header)
            print(table)


def show_materialized_views_status(args, password, site_content_url):
    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        enabled_sites = get_and_print_materialized_views_enabled_sites(server)
    #         print(server.site_id)
    #         print(server.user_id)
    #         print(server.auth_token)
    #     server._set_auth(
    #         'c422fa49-2431-42e2-aabe-a6a34601316d',
    #         'c8356eeb-56fb-42fe-bcf0-13b6d9a2bf4e',
    #         '5EQRaJ67SMaiUHYHXWTPGg|XqfssJrZP3f2BgipUHqVHX0ei0DpVh11'
    #     )

    # Individual workbooks can be enabled only when the sites they belong to are enabled too
    site_name_to_workbooks = dict()
    for site in enabled_sites:
        site_name_to_workbooks[site.name] = list()
        site_auth = TSC.TableauAuth(args.username, password, site.content_url)
        with server.auth.sign_in(site_auth):
            site_name_to_workbooks[site.name].extend(list(TSC.Pager(server.workbooks)))
    print_materialized_views_enabled_workbooks(site_name_to_workbooks)

    show_materialized_views_schedules(args, password, site_content_url)


def update_project_by_path(args, materialized_views_config, password, site_content_url):
    if args.project_path is None:
        print("Use --project_path <project path> to specify the path of the project")
        return False
    tableau_auth = TSC.TableauAuth(args.username, password, site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    project_name = args.project_path.split('/')[-1]
    with server.auth.sign_in(tableau_auth):
        if not assert_site_enabled_for_materialized_views(server, site_content_url):
            return False
        projects = [project for project in TSC.Pager(server.projects) if project.name == project_name]
        if not assert_project_valid(args.project_path, projects):
            return False

        possible_paths = get_project_paths(server, projects)
        update_project(possible_paths[args.project_path], server, materialized_views_config)
    return True


def update_project_by_name(args, materialized_views_config, password, site_content_url):
    if args.project_name is None:
        print("Use --project-name <project name> to specify the name of the project")
        return False
    tableau_auth = TSC.TableauAuth(args.username, password, site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        if not assert_site_enabled_for_materialized_views(server, site_content_url):
            return False
        # get all projects with given name
        projects = [project for project in TSC.Pager(server.projects) if project.name == args.project_name]
        if not assert_project_valid(args.project_name, projects):
            return False

        if len(projects) > 1:
            possible_paths = get_project_paths(server, projects)
            print("Project name is not unique, use '--project_path <path>'")
            print("Possible project paths:")
            print_paths(possible_paths)
            print('\n')
            return False
        else:
            update_project(projects[0], server, materialized_views_config)
    return True


def update_project(project, server, materialized_views_config):
    all_projects = list(TSC.Pager(server.projects))
    project_ids = find_project_ids_to_update(all_projects, project)
    for workbook in TSC.Pager(server.workbooks):
        if workbook.project_id in project_ids:
            workbook.materialized_views_config = materialized_views_config
            server.workbooks.update(workbook)

    print("Updated Materialized Views settings for project: {}".format(project.name))
    print('\n')


def find_project_ids_to_update(all_projects, project):
    projects_to_update = []
    find_projects_to_update(project, all_projects, projects_to_update)
    return set([project_to_update.id for project_to_update in projects_to_update])


def parse_workbook_path(file_path):
    # parse the list of project path of workbooks
    workbook_paths = sanitize_workbook_list(file_path, "path")

    workbook_path_mapping = defaultdict(list)
    for workbook_path in workbook_paths:
        workbook_project = workbook_path.rstrip().split('/')
        workbook_path_mapping[workbook_project[-1]].append('/'.join(workbook_project[:-1]))
    return workbook_path_mapping


def update_workbook_by_path(args, server, materialized_views_config):
    workbook_id_to_workbook = get_workbook_from_path(server, args)
    for workbook in workbook_id_to_workbook.values():
        try:
            workbook.materialized_views_config = materialized_views_config
            server.workbooks.update(workbook)
            return True
        except ServerResponseError as error:
            print("{}: {}".format(error.summary, error.summary))
            return False

def update_workbook(args, materialized_views_config, password, site_content_url):
    if args.path_list is None and args.name_list is None and args.workbook_path is None:
        print("Use '--path-list <filename>' or '--name-list <filename>' or --workbook-path <workbook-path> "
              "to specify the path of workbooks")
        print('\n')
        return False

    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        if not assert_site_enabled_for_materialized_views(server, site_content_url):
            return False
        if args.path_list is not None:
            workbook_path_mapping = parse_workbook_path(args.path_list)
            all_projects = {project.id: project for project in TSC.Pager(server.projects)}
            update_workbooks_by_paths(all_projects, materialized_views_config, server, workbook_path_mapping)
        elif args.name_list is not None:
            update_workbooks_by_names(args.name_list, server, materialized_views_config)
        elif args.workbook_path is not None:
            update_workbook_by_path(args, server, materialized_views_config)
    return True


def update_workbooks_by_paths(all_projects, materialized_views_config, server, workbook_path_mapping):
    table = init_table(["Project/Workbook"])
    num_enabled_workbook = 0
    for workbook_name, workbook_paths in workbook_path_mapping.items():
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         workbook_name))
        workbooks = list(TSC.Pager(server.workbooks, req_option))
        all_paths = set(workbook_paths[:])
        for workbook in workbooks:
            path = find_project_path(all_projects[workbook.project_id], all_projects, "")
            if path in workbook_paths:
                all_paths.remove(path)
                workbook.materialized_views_config = materialized_views_config
                server.workbooks.update(workbook)
                table.add_row(["{}/{}".format(workbook.project_name, workbook.name)])
                num_enabled_workbook += 1
        for path in all_paths:
            print("Cannot find workbook path: {}, each line should only contain one workbook path"
                  .format(path + '/' + workbook_name))
    header = "\nWorkbook {}: ".format("Enabled" if
                                      materialized_views_config['materialized_views_enabled'] else "Disabled");
    if num_enabled_workbook == 0:
        print("{}: None".format(header))
    else:
        print(header)
        print(table)


def update_workbooks_by_names(name_list, server, materialized_views_config):
    table = init_table(["Project/Workbook"])
    num_enabled_workbook = 0
    workbook_names = sanitize_workbook_list(name_list, "name")
    for workbook_name in workbook_names:
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         workbook_name.rstrip()))
        workbooks = list(TSC.Pager(server.workbooks, req_option))
        if len(workbooks) == 0:
            print("Cannot find workbook name: {}, each line should only contain one workbook name"
                  .format(workbook_name))
        for workbook in workbooks:
            workbook.materialized_views_config = materialized_views_config
            server.workbooks.update(workbook)
            table.add_row(["{}/{}".format(workbook.project_name, workbook.name)])
            print("Updated Materialized Views settings for workbook: {}".format(workbook.name))

    header = "\nWorkbook {}: ".format("Enabled" if
                                      materialized_views_config['materialized_views_enabled'] else "Disabled");
    if num_enabled_workbook == 0:
        print("{}: None".format(header))
    else:
        print(header)
        print(table)


def update_site(args, password, site_content_url):
    if not assert_site_options_valid(args):
        return False
    tableau_auth = TSC.TableauAuth(args.username, password, site_id=site_content_url)
    server = TSC.Server(args.server, use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        site_to_update = server.sites.get_by_content_url(site_content_url)
        site_to_update.materialized_views_mode = args.mode

        server.sites.update(site_to_update)
        print("Updated Materialized Views settings for site: {}".format(site_to_update.name))
    print('\n')
    return True


def create_materialized_views_config(args):
    materialized_views_config = dict()
    materialized_views_config['materialized_views_enabled'] = args.mode == "enable"
    materialized_views_config['run_materialization_now'] = True if args.materialize_now else False
    return materialized_views_config


def assert_site_options_valid(args):
    if args.materialize_now:
        print('"--materialize-now" only applies to workbook/project type')
        return False
    if args.mode == 'enable':
        print('For site type please choose from "disable", "enable_all", or "enable_selective"')
        return False
    return True


def assert_schedule_action_options_valid(args):
    if sum(action is not None for action in
           [args.add_to_schedule, args.remove_from_schedule, args.create_schedule, args.show_schedules]) > 1:
        print("Use one of --create-schedule, --add-to-schedule, --remove-from-schedule, or --show-schedules")
        return False

    if (args.add_to_schedule is not None or args.remove_from_schedule) and \
            (args.name_list is None and args.path_list is None and args.workbook_path is None):
        print("Use --path-list <path-list> or --name-list <name-list> or --workbook-path specify workbooks")
        return False
    elif args.create_schedule is not None and (args.weekly_interval is None and args.daily_interval is None and
                                               args.monthly_interval is None and args.hourly_interval is None):
        print("Use --hourly-interval or --daily-interval or --weekly-interval or "
              "--monthly-interval to specify the schedule type")
        return False
    else:
        return True


def assert_site_enabled_for_materialized_views(server, site_content_url):
    parent_site = server.sites.get_by_content_url(site_content_url)
    if parent_site.materialized_views_mode == "disable":
        print('Cannot update workbook/project because site is disabled for Materialized Views')
        return False
    return True


def assert_project_valid(project_name, projects):
    if len(projects) == 0:
        print("Cannot find project: {}".format(project_name))
        return False
    return True


def find_projects_to_update(project, all_projects, projects_to_update):
    # Use recursion to find all the sub-projects and enable/disable the workbooks in them
    projects_to_update.append(project)
    children_projects = [child for child in all_projects if child.parent_id == project.id]
    if len(children_projects) == 0:
        return

    for child in children_projects:
        find_projects_to_update(child, all_projects, projects_to_update)


def sanitize_workbook_list(file_name, file_type):
    if not os.path.isfile(file_name):
        print("Invalid file name '{}'".format(file_name))
        return []
    file_list = open(file_name, "r")

    if file_type == "name":
        return [workbook.rstrip() for workbook in file_list if not workbook.isspace()]
    if file_type == "path":
        return [workbook.rstrip() for workbook in file_list if not workbook.isspace()]


if __name__ == "__main__":
    main()
