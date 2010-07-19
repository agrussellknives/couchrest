module CouchRest
  class Streamer
    attr_accessor :db

    def initialize db
      @url = nil
      @db = db
    end
    
    # Stream a list, yielding one line at a time.  A list will be processed into a ruby object if it is
    # emitted in JSON compatible lines. Each line must be valid, parseable JSON, or the streamer will just
    # assume you either intend to parse it on your own, or that it isn't JSON and give you the raw results.
    # This function accepts only CouchRest style URLS <design_doc>/<list>/<view>
    def list name, params=nil, &block
      dname,lname,*vname = name.split('/')
      @url = CouchRest.paramify_url "#{@db.root}/_design/#{dname}/_list/#{lname}/#{vname.join('/')}", params
      query &block
    end
    
    # Stream a show, yielding one line at a time.  A show will be processed into a ruby object if it is
    # emitted in JSON compatible lines.  Each line must be valid, parseable JSON, or the streamer will just
    # assume you either intend to parse it on your own, or that it isn't JSON and give you the raw results
    # This function accepts only CouchRest style URLS <design_doc>/<show>/<doc_id>
    def show name, params=nil, &block
      dname,sname,*doc = name.split('/')
      @url = CouchRest.paramify_url "#{@db.root}/_design/#{dname}/_show/#{sname}/#{doc.join('/')}"
      query &block
    end
    
    # Stream a view, yielding one row at a time. Shells out to <tt>curl</tt> to keep RAM usage low when you have millions of rows.
    # This function accepts three different styles of urls.
    # Full URLs - are assumed to be correct, parameters are not added, and the URL is used exactly as passed in
    # Relative URLs - the DB root is added to the front of the urls and a passed in hash will be parameterized
    # CouchRest Style URLS - <design_doc>/<view> - passed in hash will be parameterized, and DB root added.
    def view name, params = nil, &block
      @url = case name
        when /#{@db.root}/ then name
        when /^_/ then CouchRest.paramify_url "#{@db.root}/#{name}", params
        else
          name = name.split('/')
          dname = name.shift
          vname = name.join('/')
          CouchRest.paramify_url "#{@db.root}/_design/#{dname}/_view/#{vname}", params
        end
      query &block
    end
    
    private
    
    def query
      first = nil
      IO.popen("curl --silent \"#{@url}\"") do |view|
        first = parse_first view.gets if (@url.include? '_view' or @url.include? '_all_docs') # the header row is treated differently in views
        while line = view.gets 
          row = parse_line(line)
          yield row unless row.nil? # last line "]}" is discarded in views
        end
      end
      @url = nil
      first
    end
    
    # the function can return valid JSON (if it comes in logical view "rows")
    # or something else (in newline terminated "rows")
    # if we can't parse it into json, then we just return it if it's not a
    # standard view or if it is a standard view, then we return nil.
    def parse_line line
      return nil unless line
      begin
        JSON.parse(line.match(/\{.*\}/).to_s)
      rescue JSON::ParserError
        line unless (@url.include? '_view' or @url.include? '_all_docs')
      end
    end

    def parse_first first
      return nil unless first
      line = first.split(',')[0..-2].join(',') + '}'
      JSON.parse(line)
    rescue
      nil
    end
    
  end
end
