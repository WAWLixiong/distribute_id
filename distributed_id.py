"""
CREATE TABLE id_generator (
  id int(10) NOT NULL primary key auto_increment,
  max_id bigint(20) NOT NULL COMMENT '当前最大id',
  step int(20) NOT NULL COMMENT '号段的布长',
  activity_id	varchar(15) NOT NULL COMMENT '活动id',
  version int(20) NOT NULL COMMENT '版本号',
  index idx_ai(activity_id)
);
"""

from dataclasses import dataclass


@dataclass(repr=False, eq=False)
class Model:
    id: int = None
    max_id: int = None
    step: int = None
    activity_id: str = None
    version: int = None


class ModelDao:
    def __init__(self):
        self.conn = None

    def add(self, activity_id, max_id=0, step=1000, version=0):
        sql = """
        insert ignore into id_generator(max_id, step, activity_id, version) values(%s, %s, %s, %s)
        """
        affect_rows = self.cursor.execute(sql, [max_id, step, activity_id, version])
        self.conn.commit()

    def get(self, activity_id):
        sql = """
        select id, max_id, step, activity_id, version from id_generator where activity_id = %s
        """
        self.cursor.execute(sql, [activity_id, ])
        return self.cursor.fetcone()

    def update(self, activity_id, version):
        sql = """
        update id_generator set max_id=max_id+step,version=version+1 WHERE activity_id=%s AND version=%s
        """
        affect_rows = self.cursor.execute(sql, [activity_id, version])
        self.conn.commit()
        return affect_rows


class ModelService:
    def __init__(self, activity_id):
        self.model_dao = ModelDao()
        self.activity_id = activity_id
        self.data = self.model_dao.get(self.activity_id)
        self.iter_ = self.init_data()

    def init_data(self):
        if not self.data:
            self.model_dao.add(self.activity_id)
            self.data = self.model_dao.get(self.activity_id)
        while self.model_dao.update(self.activity_id, self.data['version']):
            self.data = self.model_dao.get(self.activity_id)

        current = self.data['max_id']
        step = self.data['step']
        return iter(range(current + 1, current + step + 1))

    def generate_id(self):
        try:
            return next(self.iter_)
        except StopIteration:
            self.iter_ = self.init_data()
            return self.generate_id()
