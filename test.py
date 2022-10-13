from biflow import DAG
from biflow import Task

d = DAG('dag-One')

t1 = Task('task1')
t2 = Task('task2')
t3 = Task('task3')

d.add_tasks([t1,t2,t3])

t1.set_upstream(t2)
t1.set_upstream(t3)

t3.set_upstream(t1)

print(d.to_json())