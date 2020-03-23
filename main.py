import psycopg2
from prettytable import PrettyTable



def common_query(command):
    conn = None
    try:
        conn = psycopg2.connect(dbname='app', user='appadmin', password='rdfxf', host='192.168.23.20')
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
        conn = psycopg2.connect(dbname='app', user='appadmin', password='rdfxf', host='192.168.23.20')
        print(command)
        cur = conn.cursor()
        for item in command:
            cur.execute(item)
        conn.commit()
        rows = cur.fetchall()
        print(rows)
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
        conn = psycopg2.connect(dbname='app', user='appadmin', password='rdfxf', host='192.168.23.20')
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
    student_data = get_data(f"select name, to_char(birth, 'YYYY-MM-DD') as birth, gpa from student where course_id ={course_id} ;")
    result = PrettyTable()
    result.field_names = student_data["header"]
    for item in student_data["payload"]:
        result.add_row(item)
    print(result)


def create_tables():
    common_query("create table if not exists course (id serial primary key, name character varying(100) not null)")
    common_query("create table if not exists student (id serial primary key, name character varying(100) not null,\
    gpa numeric(10,2), birth timestamp with time zone, course_id integer, FOREIGN KEY (course_id) REFERENCES course (id));")


def add_students(values):
    query = []
    for item in values:
        columns_name = ','.join(item.keys())
        data = "'" + "','".join(item.values()) + "'"
        query.append(f"insert into student ({columns_name}) values ({data})")
    batch_query(query)


def get_student(student_id):
    student_data = get_data(f"select name, to_char(birth, 'YYYY-MM-DD') as birth, gpa from student where id ={student_id} ;")
    result = PrettyTable()
    result.field_names = student_data["header"]
    for item in student_data["payload"]:
        result.add_row(item)
    print(result)


if __name__ == '__main__':
    create_tables()
    add_course({"name": "First"})
    add_course({"name": "Second"})
    add_course({"name": "Third"})
    add_course({"name": "Fourth"})
    add_course({"name": "Fifth"})

    students = [{"name": "Vasya", "birth": "1990-09-01", "course_id": "1"},
                {"name": "Jonh Dow", "birth": "1991-09-01", "course_id": "2"},
                {"name": "Key Smith", "birth": "1992-09-01", "course_id": "3"},
                {"name": "Bill Gilbert", "birth": "1993-09-01", "course_id": "4"}]

    add_students(students)
    get_students("1")
    get_student("1")



