import psycopg2;


from themuserb import cfg;
from themuserb.jobsdb import JobsDBSchema;



query \
  = """
      SELECT count(*)
        FROM job
       WHERE '2016-09-01 0:00:00' <= publication_date
         AND publication_date < '2016-10-01 0:00:00'
           ;
    """;



def main( host, user, password, dbname ):

  connstr \
    =   "host={} ".format( host ) \
      + "user={} ".format( user ) \
      + "password={} ".format( password ) \
      + "dbname={} ".format( dbname );

  conn = psycopg2.connect( connstr );
  try:

    print( query );

    result = None;

    cursor = conn.cursor();
    try:
      cursor.execute( query );
      result = list( cursor.fetchall() )[ 0 ][ 0 ];
    finally:
      cursor.close();

  finally:
    conn.commit();
    conn.close();

  print( "-->", result );
  print();



if __name__ == "__main__":

  import sys;
  main( *sys.argv );
