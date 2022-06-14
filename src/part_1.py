import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import close_all_sessions
import sys


def part_1():
    # TASKS PART 1:
    # 1
    try:
        close_all_sessions()
        engine = create_engine(
            'postgresql://postgres:pass_pgsql@localhost:5432/ortex')

        df = pd.read_sql_query('select exchange as ESCHANGE, COUNT (*) as QUANTITY ' +
                               'from public.transactions ' +
                               'group by exchange ' +
                               'order by COUNT (*) desc ' +
                               'limit 3', con=engine)
        df.to_csv('part_1_task_1.csv', index=False)

# 2
        df = pd.read_sql_query('select companyname , sum(valueeur) as value_sum ' +
                               'from transactions t '
                               'where extract(month from inputdate) = 8 ' +
                               'group by companyname  ' +
                               'order by sum(valueeur) desc  ' +
                               'limit 2 ', con=engine)
        df.to_csv('part_1_task_2.csv', index=False)

# 3
        sql_string = (
            """ select  to_char(inputdate, 'Month') as month_2017,
                                    count(*) as month_quantity,
                                    round ( cast(float8  (count(*)/y.count_year::float)*100  as numeric) ,2) as perc_year
                            from transactions t,
                                    (
                                    select  count(*) as count_year 
                                    from transactions t 
                                    where extract(year from inputdate) = 2017
                                    and  tradesignificance = 3
                                    ) y  		
                            where extract(year from inputdate) = 2017
                            and  tradesignificance = 3 
                            group by extract(month from inputdate),y.count_year ,TO_CHAR(inputdate, 'Month') 
                            order by extract(month from inputdate)
                            """
        )
        df = pd.read_sql_query(sql_string, con=engine)
        df.to_csv('part_1_task_3.csv', index=False)
# TASKS PART 2:
# 1
        sql_string = (""" select  "source" as source_desc,
                                     sum(shares) as shared_per_trans
                               from transactions t
                              where t.transactiontype  = 'Buy' 
                              group by "source" ,shares 
                              order by sum(shares)  desc  
                                 limit 3	"""
                      )

        df = pd.read_sql_query(sql_string, con=engine)
        df.to_csv('part_2_task_1.csv', index=False)
# 2
        sql_string = (""" select t.currency, sum(t.value) value_sum   
                               from transactions t 
                               group by t.currency, t.value  
                               order by sum(t.value) desc 
                               limit 3	"""
                      )

        df = pd.read_sql_query(sql_string, con=engine)
        df.to_csv('part_2_task_2.csv', index=False)
# 3
        sql_string = (""" select count(*) as  total_transactions 
                               from transactions t
                               where inputdate + 14 > t.tradedate	"""
                      )

        df = pd.read_sql_query(sql_string, con=engine)
        df.to_csv('part_2_task_3.csv', index=False)

    except SQLAlchemyError as e:
        print(str(e.__dict__['orig']))
        raise
