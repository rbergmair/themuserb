from pprint import pprint;
from requests import get as requests_get;
from json import loads as json_loads;
from datetime import datetime;
from urllib.parse import urlparse;


from themuserb import cfg;


THEMUSE_JOBS_BASEURL \
  =   cfg.THEMUSE_BASEURL \
    + "/jobs";



def themuse_download_jobs_pages( no_pages_to_download=None ):

  if no_pages_to_download is not None:
    assert isinstance( no_pages_to_download, int );
    if no_pages_to_download == 0:
      return;

  no_pages_available = None;
  page = 0;

  while True:

    api_url \
      =   THEMUSE_JOBS_BASEURL + "?" \
        + "page={}".format( page );

    response \
      = requests_get(
            api_url
          );

    assert \
      response.status_code == 200;

    assert \
      response.headers['content-type'] == "application/json; charset=UTF-8";

    r = json_loads( response.text );

    assert r.get( "page", None ) == page;
    assert "page_count" in r;
    assert "results" in r;

    if no_pages_available is None:
      no_pages_available = r[ "page_count" ];
    else:
      assert no_pages_available == r[ "page_count" ];

    for result in r[ "results" ]:
      yield result;

    page += 1;
    if page >= no_pages_available:
      break;
    if no_pages_to_download is not None:
      if page >= no_pages_to_download:
        break;



class TheMuse:

  JOBS_FIELDS \
    = { "model_type",
        "id",
        "publication_date",
        "short_name",
        "name",
        "type",
        "refs",
        "company",
        "locations",
        "levels",
        "categories",
        "tags" };

  COMPANIES_FIELDS \
    = { "id",
        "short_name",
        "name" };
  
  def __init__( self ):

    self._job_by_id = {};
    self._company_by_id = {};


  def download_jobs( self, no_pages_to_download=None ):

    for result in themuse_download_jobs_pages( no_pages_to_download ):

      assert \
           ( set(result.keys()) & self.JOBS_FIELDS ) \
        == self.JOBS_FIELDS;

      assert \
        result.get( "model_type", None ) == "jobs";

      print( "--" );
      print( "id", result[ "id" ] );
      print( "short_name", result[ "short_name" ] );
      print( "name", result[ "name" ] );

      assert set( result[ "refs" ].keys() ) == { "landing_page" };
      result[ "landing_page" ] = result[ "refs" ][ "landing_page" ];
      del result[ "refs" ];
      urlparse( result[ "landing_page" ] );
      print( "landing_page", result[ "landing_page" ] );

      location_names = set();
      for location in result[ "locations" ]:
        assert set( location.keys() ) == { "name" };
        assert location[ "name" ] not in location_names;
        location_names.add( location["name"] );
      del result[ "locations" ];
      result[ "location_names" ] = location_names;
      print(  "location_names", result[ "location_names" ] );

      level_snames = set();
      for level in result[ "levels" ]:
        assert set( level.keys() ) == { "short_name", "name" };
        if level[ "short_name" ] == "entry":
          assert level[ "name" ] == "Entry Level";
        elif level[ "short_name" ] == "mid":
          assert level[ "name" ] == "Mid Level";
        elif level[ "short_name" ] == "senior":
          assert level[ "name" ] == "Senior Level";
        elif level[ "short_name" ] == "internship":
          assert level[ "name" ] == "Internship";
        else:
          assert False;
        assert level["short_name"] not in level_snames;
        level_snames.add( level["short_name"] );
      del result[ "levels" ];
      result[ "level_snames" ] = level_snames;
      print( "level_snames", result[ "level_snames" ] );

      category_names = set();
      for category in result[ "categories" ]:
        assert set( category.keys() ) == { "name" };
        assert category[ "name" ] not in category_names;
        category_names.add( category[ "name" ] );    
      del result[ "categories" ];
      result[ "category_names" ] = category_names;
      print( "category_names", result[ "category_names" ] );

      tag_snames = set();
      for tag in result[ "tags" ]:
        assert set( tag.keys() ) == { "short_name", "name" };
        try:
          if tag[ "short_name" ] == "fortune-1000-companies":
            assert tag[ "name" ] == "Fortune 1000";
          elif tag[ "short_name" ] == "fast-growing-companies":
            assert tag[ "name" ] == "Fast Growing Companies";        
          elif tag[ "short_name" ] == "yc-companies":
            assert tag[ "name" ] == "YC Companies";
          else:
            assert False;
        except:
          print( tag );
          raise;
        assert tag[ "short_name" ] not in tag_snames;
        tag_snames.add( tag["short_name"] );
      del result[ "tags" ];
      result[ "tag_snames" ] = tag_snames;
      print( "tag_snames", result[ "tag_snames" ] );

      print( "company", result[ "company" ] );

      company = result[ "company" ];
      assert \
           ( set(company.keys()) & self.COMPANIES_FIELDS ) \
        == self.COMPANIES_FIELDS;
      company_id = company[ "id" ];
      company_sname = company[ "short_name" ];
      company_name = company[ "name" ];
      company = ( company_sname, company_name );
      if company_id in self._company_by_id:
        assert self._company_by_id[ company_id ] == company;
      else:
        self._company_by_id[ company_id ] = company;
      del result[ "company" ];
      result[ "company_id" ] = company_id;
      print( "company_id", result[ "company_id" ] );

      pdate = result[ "publication_date" ];
      assert pdate.endswith( "Z" );
      pdate = pdate[ :-1 ];
      if len( pdate ) == len( "2016-12-21T10:32:10" ):
        microseconds = "000000"        
      else:
        ( pdate, fractional_seconds ) = pdate.split( "." );
        microseconds = fractional_seconds.ljust(6,"0");
      pdate \
        = datetime.strptime(
              pdate + "." + microseconds,
              "%Y-%m-%dT%H:%M:%S.%f"
            );
      result[ "publication_date" ] = pdate;
      print( "publication_date", result[ "publication_date" ] );

      assert result[ "type" ] in { 'external', 'native' };
      print( "type", result[ "type" ] );

      job_id = result[ "id" ];
      del result[ "id" ];
      assert job_id not in self._job_by_id;
      self._job_by_id[ job_id ] = result;


  def get_jobs( self ):

    for job_id in sorted( self._job_by_id.keys() ):

      job = self._job_by_id[ job_id ];

      publication_date = job[ "publication_date" ];

      short_name = job[ "short_name" ];

      name = job[ "name" ];

      if job[ "type" ] == "external":
        type_ = "X";
      elif job[ "type" ] == "native":
        type_ = "N";
      else:
        assert False;

      company_id = job[ "company_id" ];

      landing_page = job[ "landing_page" ];

      yield \
        ( job_id,
          publication_date,
          short_name,
          name,
          type_,
          company_id,
          landing_page );


  def get_job_locations( self ):

    for job_id in sorted( self._job_by_id.keys() ):
      job = self._job_by_id[ job_id ];
      for location_name in job[ "location_names" ]:
        yield ( job_id, location_name );


  def get_job_levels( self ):

    for job_id in sorted( self._job_by_id.keys() ):

      job = self._job_by_id[ job_id ];

      for level_sname in job[ "level_snames" ]:

        level_code = None;
        if level_sname == "entry":
          level_code = 'E';
        elif level_sname == "mid":
          level_code = 'M';
        elif level_sname == "senior":
          level_code = 'S';
        elif level_sname == "internship":
          level_code = 'I';
        else:
          assert False;

        yield ( job_id, level_code );


  def get_job_categories( self ):

    for job_id in sorted( self._job_by_id.keys() ):
      job = self._job_by_id[ job_id ];
      for category_name in job[ "category_names" ]:
        yield ( job_id, category_name );


  def get_job_tags( self ):

    for job_id in sorted( self._job_by_id.keys() ):

      job = self._job_by_id[ job_id ];

      for tag_sname in job[ "tag_snames" ]:

        tag_code = None;
        
        try:
          if tag_sname == "fortune-1000-companies":
            tag_code = "F";
          elif tag_sname == "fast-growing-companies":
            tag_code = "G";
          elif tag_sname == "yc-companies":
            tag_code = "Y";
          else:
            assert False;
        except:
          print( tag_sname );
          raise;

        yield ( job_id, tag_code );


  def get_companies( self ):

    for company_id in sorted( self._company_by_id.keys() ):
      ( sname, name ) = self._company_by_id[ company_id ];
      yield ( company_id, sname, name );
