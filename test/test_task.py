import unittest
import os
import requests_mock
import tableauserverclient as TSC
from tableauserverclient.models.task_item import TaskItem
from tableauserverclient.datetime_helpers import parse_datetime

TEST_ASSET_DIR = os.path.join(os.path.dirname(__file__), "assets")

GET_XML_NO_WORKBOOK = os.path.join(TEST_ASSET_DIR, "tasks_no_workbook_or_datasource.xml")
GET_XML_WITH_WORKBOOK = os.path.join(TEST_ASSET_DIR, "tasks_with_workbook.xml")
GET_XML_WITH_DATASOURCE = os.path.join(TEST_ASSET_DIR, "tasks_with_datasource.xml")
GET_XML_WITH_WORKBOOK_AND_DATASOURCE = os.path.join(TEST_ASSET_DIR, "tasks_with_workbook_and_datasource.xml")
GET_XML_MATERIALIZEVIEWS_TASK = os.path.join(TEST_ASSET_DIR, "tasks_with_materializeviews_task.xml")


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.server = TSC.Server("http://test")
        self.server.version = '3.8'

        # Fake Signin
        self.server._site_id = "dad65087-b08b-4603-af4e-2887b8aafc67"
        self.server._auth_token = "j80k54ll2lfMZ0tv97mlPvvSCRyD0DOM"

        # default task type is extractRefreshes
        self.baseurl = "{}/{}".format(self.server.tasks.baseurl, "extractRefreshes")

    def test_get_tasks_with_no_workbook(self):
        with open(GET_XML_NO_WORKBOOK, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get()

        task = all_tasks[0]
        self.assertEqual(None, task.target)

    def test_get_tasks_with_workbook(self):
        with open(GET_XML_WITH_WORKBOOK, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get()

        task = all_tasks[0]
        self.assertEqual('c7a9327e-1cda-4504-b026-ddb43b976d1d', task.target.id)
        self.assertEqual('workbook', task.target.type)

    def test_get_tasks_with_datasource(self):
        with open(GET_XML_WITH_DATASOURCE, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get()

        task = all_tasks[0]
        self.assertEqual('c7a9327e-1cda-4504-b026-ddb43b976d1d', task.target.id)
        self.assertEqual('datasource', task.target.type)

    def test_get_tasks_with_workbook_and_datasource(self):
        with open(GET_XML_WITH_WORKBOOK_AND_DATASOURCE, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get()

        self.assertEqual('workbook', all_tasks[0].target.type)
        self.assertEqual('datasource', all_tasks[1].target.type)
        self.assertEqual('workbook', all_tasks[2].target.type)

    def test_get_task_with_schedule(self):
        with open(GET_XML_WITH_WORKBOOK, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get()

        task = all_tasks[0]
        self.assertEqual('c7a9327e-1cda-4504-b026-ddb43b976d1d', task.target.id)
        self.assertEqual('workbook', task.target.type)
        self.assertEqual('b60b4efd-a6f7-4599-beb3-cb677e7abac1', task.schedule_id)

    def test_delete(self):
        with requests_mock.mock() as m:
            m.delete(self.baseurl + '/c7a9327e-1cda-4504-b026-ddb43b976d1d', status_code=204)
            self.server.tasks.delete('c7a9327e-1cda-4504-b026-ddb43b976d1d')

    def test_delete_missing_id(self):
        self.assertRaises(ValueError, self.server.tasks.delete, '')

    def test_get_materializeviews_tasks(self):
        with open(GET_XML_MATERIALIZEVIEWS_TASK, "rb") as f:
            response_xml = f.read().decode("utf-8")
        with requests_mock.mock() as m:
            m.get('{}/{}'.format(
                self.server.tasks.baseurl, TaskItem.Type.MaterializeViews), text=response_xml)
            all_tasks, pagination_item = self.server.tasks.get(task_type=TaskItem.Type.MaterializeViews)

        task = all_tasks[0]
        self.assertEqual('a462c148-fc40-4670-a8e4-39b7f0c58c7f', task.target.id)
        self.assertEqual('workbook', task.target.type)
        self.assertEqual('b22190b4-6ac2-4eed-9563-4afc03444413', task.schedule_id)
        self.assertEqual(parse_datetime('2019-12-09T22:30:00Z'), task.schedule_item.next_run_at)
        self.assertEqual(parse_datetime('2019-12-09T20:45:04Z'), task.last_run_at)

    def test_delete(self):
        with requests_mock.mock() as m:
            m.delete('{}/{}/{}'.format(
                self.server.tasks.baseurl, TaskItem.Type.MaterializeViews,
                'c9cff7f9-309c-4361-99ff-d4ba8c9f5467'), status_code=204)
            self.server.tasks.delete('c9cff7f9-309c-4361-99ff-d4ba8c9f5467',
                                     TaskItem.Type.MaterializeViews)