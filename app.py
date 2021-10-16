from flask import Flask, request
import sqlite3
from flask_restful import Api, Resource
from db_adapter import get_issue, get_issue_list, get_issue_count, put_issue, \
    get_program_list, get_user_list, \
    create_task, get_task, get_task_list, get_issue_task_list, get_task_count, put_task

app = Flask(__name__)
api = Api(app)


@app.after_request
def apply_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    return response


def _make_issue_record_insane(record):
    skipped_fields = ['id', 'longitude', 'latitude']
    row = {
        'id': record['id'],
    }
    if 'longitude' in record.keys():
        row['longitude'] = record['longitude']
    if 'latitude' in record.keys():
        row['latitude'] = record['latitude']

    fields = []
    for key, value in record.items():
        if key in skipped_fields:
            continue
        fields.append({
            'name': key,
            'value': value
        })
    row['properties'] = fields
    return row


class UserList(Resource):
    def get(self):
        return get_user_list()


class ProgramList(Resource):
    def get(self):
        return get_program_list()


class IssueList(Resource):
    def get(self):
        args = request.args
        limit = args.get('limit', 20)
        limit = int(limit)
        offset = args.get('offset', 0)
        offset = int(offset)
        issues = get_issue_list(limit, offset)
        data = list(map(_make_issue_record_insane, issues))
        total = get_issue_count()
        return data, 200, {'X-Total': total, 'X-Limit': limit, 'X-Offset': offset}


class Issue(Resource):
    def get(self, issue_id):
        record = get_issue(issue_id)
        return _make_issue_record_insane(record)

    def put(self, issue_id):
        raw_data = request.get_json()
        data = {
            'id': issue_id,
            'longitude': raw_data['longitude'],
            'latitude': raw_data['latitude']
        }
        properties = raw_data['properties']
        for prop in properties:
            data[prop['name']] = prop['value']
        fields = ['id', 'longitude', 'latitude', 'name', 'description', 'price', 'program']
        keys = data.keys()
        if sorted(fields) != sorted(keys):
            resp = {
                'code': 400,
                'message': 'Invalid data format'
            }
            return resp, 400
        try:
            updated = put_issue(data)
            updated = _make_issue_record_insane(updated)
        except sqlite3.IntegrityError as e:
            resp = {
                'code': 400,
                'message': str(e)
            }
            return resp, 400
        return updated


class IssueStartTask(Resource):
    def post(self, issue_id):
        try:
            task = create_task(issue_id)
        except sqlite3.IntegrityError as err:
            resp = {
                'code': 400,
                'message': str(err)
            }
            return resp, 400
        return task


class IssueTaskList(Resource):
    def get(self, issue_id):
        return get_issue_task_list(issue_id)


class Task(Resource):
    def get(self, task_id):
        return get_task(task_id)

    def put(self, task_id):
        task = get_task(task_id)
        post_data = request.get_json()
        if task['issue'] != post_data['issue']:
            resp = {
                'code': 400,
                'message': 'Field issue is readonly'
            }
            return resp, 400
        try:
            data = {**post_data, 'id': task_id, 'issue': task['issue']}
            updated = put_task(data)
        except sqlite3.IntegrityError as err:
            resp = {
                'code': 400,
                'message': str(err)
            }
            return resp, 400
        return updated


class TaskList(Resource):
    def get(self):
        args = request.args
        limit = args.get('limit', 20)
        limit = int(limit)
        offset = args.get('offset', 0)
        offset = int(offset)
        data = get_task_list(limit, offset)
        total = get_task_count()
        return data, 200, {'X-Total': total, 'X-Limit': limit, 'X-Offset': offset}


api.add_resource(UserList, '/api/user')
api.add_resource(ProgramList, '/api/program')
api.add_resource(IssueList, '/api/issue')
api.add_resource(Issue, '/api/issue/<issue_id>')
api.add_resource(IssueStartTask, '/api/issue/<issue_id>/start-task')
api.add_resource(IssueTaskList, '/api/issue/<issue_id>/task')
api.add_resource(TaskList, '/api/task')
api.add_resource(Task, '/api/task/<task_id>')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8084)