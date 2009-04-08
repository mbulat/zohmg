# user's mapper.
def map(key, value):
    import sys
    from lfm.data.parse import web

    try: log = web.Log(value)
    except: return
    ua = web.UserAgent()

    try: # been seeing too many AttributeErrors lately..
        ts = log.timestamp.ymd()
        dimensions = {'country'   : log.country(),
                      'domain'    : log.domain,
                      'useragent' : ua.classify(log.agent),
                      'usertype'  : ("user", "anon")[log.userid == None]
                      }
        values = {'pageviews' : 1}

    except AttributeError:
        sys.stderr.write("AttributeError!\n")
        return
    
    yield ts, dimensions, values
