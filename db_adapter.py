import os
import sqlite3
import random

PATH = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(PATH, 'db.sqlite')


def _get_count(table):
    sql = 'SELECT count(1) FROM {}'.format(table)
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    cursor.execute(sql)
    count = cursor.fetchone()
    return count[0]


def _generate_description():
    lorem = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit. Amet, delectus eius esse est, maiores maxime nostrum omnis, pariatur quae quasi quibusdam rem ullam! Assumenda consequatur deleniti dignissimos dolor ea enim facere fuga fugit illo inventore magni minus molestias nam non obcaecati possimus recusandae saepe sequi, sint velit veniam voluptatem voluptatum.'
    lorem_words = lorem.split(' ')
    count = len(lorem_words) - 1
    word_count = random.randint(round(count / 6), count)
    result = []
    for i in range(word_count):
        num = random.randint(0, count)
        result.append(lorem_words[num])
    return " ".join(result)


def _generate_program():
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    sql = "SELECT * FROM program"
    cursor.execute(sql)
    raw_data = cursor.fetchall()
    programs = list(map(lambda x: format_record(cursor, x), raw_data))
    num = random.randint(0, len(programs) - 1)
    return programs[num]['id']


def _generate_issue():
    number = random.randint(0, 6000)
    name = 'проблема - {}'.format(number)
    description = _generate_description()
    price = random.random() * 10000000
    program = _generate_program()
    longitude = random.randint(29, 100)
    latitude = random.randint(41, 81)
    return [
        name, description, price, longitude, latitude, program
    ]


def populate_issues():
    if _get_count('issue') > 0:
        return
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    sql = "INSERT INTO issue(name, description, price, longitude, latitude, program) values(?,?,?,?,?,?)"
    for i in range(300):
        issue = _generate_issue()
        cursor.execute(sql, issue)
    connection.commit()
    connection.close()


def populate_program():
    if _get_count('program') > 0:
        return
    l = [
        'Программа 1',
        'Программа 2',
        'LTE покрытие',
        'Подземка',
        'Программа 3',
        'Программа 4',
        'Программа 5',
    ]
    sql = "INSERT INTO program(name) values(?)"
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    for p in l:
        cursor.execute(sql, [p])
    connection.commit()
    connection.close()


def populate_users():
    if _get_count('user') > 0:
        return
    users = [
        ['Macey', 'Bowers'],
        ['Alan', 'Spencer'],
        ['Gunner', 'Buchanan'],
        ['Marina', 'Stevens'],
        ['Journey', 'Durham'],
        ['Hana', 'Beasley'],
        ['Charlotte', 'Stephenson'],
        ['Reagan', 'Bush'],
        ['Hadley', 'Sheppard'],
        ['Kolton', 'Ibarra'],
        ['Daniel', 'Lawson'],
        ['Raegan', 'Norris'],
    ]
    sql = "INSERT INTO user(first_name, last_name) values(?, ?)"
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    for user in users:
        cursor.execute(sql, user)
    connection.commit()
    connection.close()


def init_db():
    if not os.path.exists(DB):
        handler = open(DB, 'w')
        handler.close()
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS program(
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL                 
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user(
        id INTEGER NOT NULL PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL                 
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS task(
        id INTEGER NOT NULL PRIMARY KEY,
        issue INTEGER NOT NULL,
        name TEXT NOT NULL,
        user INTEGER                
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS issue(
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        price INTEGER NOT NULL,
        longitude REAL NOT NULL,
        latitude REAL NOT NULL,
        program INTEGER NOT NULL     
    )
    """)
    connection.commit()
    connection.close()


def connect():
    init_db()
    populate_program()
    populate_users()
    populate_issues()
    return sqlite3.connect(DB)


def format_record(cursor, record):
    names = [description[0] for description in cursor.description]
    data = {}
    for column in zip(names, record):
        data[column[0]] = column[1]
    return data


def _get_list(sql, params):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(sql, params)
    raw_data = cursor.fetchall()
    connection.close()
    records = list(map(lambda x: format_record(cursor, x), raw_data))
    return records


def _get_one(sql, params):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(sql, params)
    raw_data = cursor.fetchone()
    connection.close()
    if raw_data is None:
        return raw_data
    return format_record(cursor, raw_data)


def get_program_list():
    sql = "SELECT * FROM program ORDER BY id"
    return _get_list(sql, [])


def get_program(id):
    sql = "SELECT * FROM program WHERE id = ?"
    try:
        _get_one(sql, [id])
    except Exception as e:
        print(e)
    return _get_one(sql, [id])


def get_user_list():
    sql = "SELECT * FROM user ORDER BY id"
    return _get_list(sql, [])


def get_user(id):
    sql = "SELECT * FROM user WHERE id = ?"
    return _get_one(sql, [id])


def get_issue_list(limit, offset):
    sql = "SELECT id, name, price FROM issue ORDER BY id LIMIT ? OFFSET ?"
    return _get_list(sql, [limit, offset])


def get_issue(id):
    sql = "SELECT * FROM issue WHERE id = ?"
    return _get_one(sql, [id])


def get_issue_count():
    return _get_count('issue')


def get_task_count():
    return _get_count('task')


def put_issue(data):
    params = [
        data['name'],
        data['description'],
        data['price'],
        data['program'],
        data['id']
    ]
    program = get_program(data['program'])
    if not program:
        raise sqlite3.IntegrityError('Program not exists')
    sql = "UPDATE issue SET name = ? , description = ? , price = ?, program = ? WHERE id = ?"
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(sql, params)
    connection.commit()
    connection.close()
    return get_issue(data['id'])


def create_task(issue_id):
    issue = get_issue(issue_id)
    if issue is None:
        raise sqlite3.IntegrityError('Issue does not exist')
    count = _get_count('task')
    name = 'задача - #{}'.format(count + 1)
    sql = "INSERT INTO task (name, issue) VALUES (?,?)"
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(sql, [name, issue_id])
    new_id = cursor.lastrowid
    connection.commit()
    connection.close()
    return get_task(new_id)


def get_task_list(limit, offset):
    sql = "SELECT * FROM task ORDER BY id LIMIT ? OFFSET ?"
    return _get_list(sql, [limit, offset])


def get_issue_task_list(issue_id):
    sql = "SELECT * FROM task WHERE issue = ? ORDER BY id"
    return _get_list(sql, [issue_id])


def get_task(id):
    sql = "SELECT * FROM task WHERE id = ?"
    return _get_one(sql, [id])


def put_task(data):
    id = data['id']
    name = data['name']
    user = get_user(data['user'])
    if not user:
        raise sqlite3.IntegrityError('User not exists')
    sql = "UPDATE task SET name = ? , user = ? WHERE id = ?"
    params = [
        name, user['id'], id
    ]
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(sql, params)
    connection.commit()
    connection.close()
    return get_task(data['id'])