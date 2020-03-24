import psycopg2
from prettytable import PrettyTable


def common_query(command):
    conn = None
    try:
        conn = psycopg2.connect(dbname='', user='', password='', host='192.168.x.x')
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
        rows = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def batch_query(command):
    conn = None
    try:
        conn = psycopg2.connect(dbname='', user='', password='', host='192.168.x.x')
        cur = conn.cursor()
        for item in command:
            cur.execute(item)
        conn.commit()
        rows = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)
    finally:
        if conn is not None:
            conn.close()


def get_data(command):
    result = {}
    conn = None
    try:
        conn = psycopg2.connect(dbname='', user='', password='', host='192.168.x.x')
        cur = conn.cursor()
        cur.execute(command)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.commit()
        result.update({"header": colnames})
        current_value = []
        for item in rows:
            current_value.append(item)
            result.update({"payload": current_value})
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return result


def get_query(values):
    columns_name = ','.join(values.keys())
    data = "'" + "','".join(values.values()) + "'"
    query = f"insert into course ({columns_name}) values ({data})"
    return query


def add_course(values):
    common_query(get_query(values))


def get_students(course_id):
    student_data = get_data(
        f"select name,birth from student left join student_course on student_course.student_id=student.id where \
        student_course.course_id={course_id};")
    # select name,birth from student left join student_course on student_course.student_id=student.id where student_course.course_id=2;
    # student_data = get_data(f"select name, to_char(birth, 'YYYY-MM-DD') as birth, gpa from student where course_id ={course_id} ;")
    result = PrettyTable()
    result.field_names = student_data["header"]
    for item in student_data["payload"]:
        result.add_row(item)
    print(result)


def create_tables():
    common_query(
        "create table if not exists course (id integer not null, name character varying(100) not null, UNIQUE (id));")
    common_query("create table if not exists student (id integer not null, name character varying(100) not null,\
    gpa numeric(10,2), birth timestamp with time zone, UNIQUE (id));")
    common_query("create table student_course (id serial primary key, course_id integer, student_id integer, \
    FOREIGN KEY (course_id) REFERENCES course (id), FOREIGN KEY (student_id) REFERENCES student (id));")


def add_students(values, course_id=None):
    query = []
    for item in values:
        columns_name = ','.join(item.keys())
        data = "'" + "','".join(item.values()) + "'"
        query.append(f"insert into student ({columns_name}) values ({data})")
        if course_id != None:
            student_id = item["id"]
            query.append(f"insert into student_course (student_id,course_id) values ({student_id},{course_id})")
    batch_query(query)


def get_student(student_id):
    student_data = get_data(
        f"select name, to_char(birth, 'YYYY-MM-DD') as birth, gpa from student where id ={student_id} ;")
    result = PrettyTable()
    result.field_names = student_data["header"]
    for item in student_data["payload"]:
        result.add_row(item)
    print(result)


def add_student(values):
    student = []
    student.append(values)
    add_students(student)


if __name__ == '__main__':
    create_tables()
    add_course({"id": "1", "name": "First"})
    add_course({"id": "2", "name": "Second"})
    add_course({"id": "3", "name": "Third"})
    add_course({"id": "4", "name": "Fourth"})
    add_course({"id": "5", "name": "Fifth"})

    students = [{"id": "1", "name": "Vasya", "birth": "1990-09-01"},
                {"id": "2", "name": "Jonh Dow", "birth": "1991-09-01"}]
    # создаем студентов и записываем на курс с id=1
    add_students(students, 1)

    students = [{"id": "3", "name": "Key Smith", "birth": "1992-09-01"},
                {"id": "4", "name": "Bill Gilbert", "birth": "1993-09-01"}]
    # создаем студентов и записываем на курс с id=2
    add_students(students, 2)

    # создаем студента
    add_student({"name": "John Snow", "birth": "1970-09-01"})

    # запрос студентов с курса id=1
    get_students("1")
    # запрос студента с id=2
    get_student("2")
