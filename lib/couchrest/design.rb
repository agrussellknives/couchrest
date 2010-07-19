module CouchRest  
  class Design < Document
    def view_by *keys
      opts = keys.pop if keys.last.is_a?(Hash)
      opts ||= {}
      self['views'] ||= {}
      method_name = "by_#{keys.join('_and_')}"
      
      if opts[:map]
        view = {}
        view['map'] = opts.delete(:map)
        if opts[:reduce]
          view['reduce'] = opts.delete(:reduce)
          opts[:reduce] = false
        end
        self['views'][method_name] = view
      else
        doc_keys = keys.collect{|k|"doc['#{k}']"} # this is where :require => 'doc.x == true' would show up
        key_emit = doc_keys.length == 1 ? "#{doc_keys.first}" : "[#{doc_keys.join(', ')}]"
        guards = opts.delete(:guards) || []
        guards += doc_keys.map{|k| "(#{k} != null)"}
        map_function = <<-JAVASCRIPT
function(doc) {
  if (#{guards.join(' && ')}) {
    emit(#{key_emit}, null);
  }
}
JAVASCRIPT
        self['views'][method_name] = {
          'map' => map_function
        }
      end
      self['views'][method_name]['couchrest-defaults'] = opts unless opts.empty?
      method_name
    end
    
    # Create a list function on a design document by specifiying a string with
    # a compilable function in it.  If no list function is supplied, then
    # a simple echo function is used. Returns the name of the function - which
    # will always be "echo" if the default function is used
    def list_using name, func = nil
      raise ArgumentError, "_list functions must supply a name" unless name && name.length > 0
      self['lists'] ||= {}
      if func
        self['lists'][name] = func
        name
      else
        self['lists'][name] = <<-JAVASCRIPT
function(head,req) {
  var row;
  start({"headers":{"Content-Type":"application/json"}});
  send('[');
  row = getRow()
  while(true) {
    if(row) {
      send(JSON.stringify(row));
    }
    row = getRow();
    if(row) {
      send(',');
    } else {
      break;
    }
  }
  send(']');
}
JAVASCRIPT
        :echo
      end
    end
    
    # Create a show function on a design document by specifiying a string with
    # a compilable function in it.  If no show function is supplied, then
    # a simple echo function is used. Returns the name of the function - which
    # will always be "echo" if the default function is used
    def show_using name, func = nil
      raise ArgumentError, "_show functions must supply a name" unless name && name.length > 0
      self['shows'] ||= {}
      if func
        self['shows'][name] = func
        name
      else
        self['shows'][name] = <<-JAVASCRIPT
function(doc,req) {
  return { 'json' : doc };  
}
JAVASCRIPT
        :echo
      end
    end
      
    
    # Dispatches to any named view.
    # (using the database where this design doc was saved)
    def view view_name, query={}, &block
      query_on database, view_name, :view, query, &block
    end
     
    # Dispatches to named show
    # (using the database where this design doc was saved)
    def show show_name, query={}, &block
      query_on database, show_name, :show, query, &block
    end
    
    # Dispatches to named list
    # (using the database where this design doc was saved)
    # to use a list function from a different design doc, specify it before the list function
    # <list_name>/<design_doc>/<view>
    def list list_name, query={}, &block
      if (list_name.include? name) and (list_name.split('/').size == 3) then
        raise ArgumentError, "external _design doc specified is same as primary _design doc"
      end 
      query_on database, list_name, :list, query, &block
    end

    # Dispatches to any named design doc function in a specific database
    def query_on db, view_name, method = :view, query={}, &block
      view_name = view_name.to_s
      view_slug = "#{name}/#{view_name}"
      key = method.to_s + "s"
      defaults = (self[key][view_name] && self[key][view_name]["couchrest-defaults"]) || {}
      db.send(method, view_slug, defaults.merge(query), &block)
    end
    
    # Calls to query_on with a preset method.  Show is not supported.
    def list_on db, view_name, query={}, &block
      query_on db, view_name, :list, query, &block
    end
    
    def view_on db, view_name, query={}, &block
      query_on db, view_name, :view, query, &block
    end
    
    def name
      id.sub('_design/','') if id
    end

    def name= newname
      self['_id'] = "_design/#{newname}"
    end

    def save
      raise ArgumentError, "_design docs require a name" unless name && name.length > 0
      super
    end

    private

    # returns stored defaults if the there is a view named this in the design doc
    def has_view?(view)
      view = view.to_s
      self['views'][view] &&
        (self['views'][view]["couchrest-defaults"] || {})
    end

    def fetch_view view_name, opts, &block
      database.view(view_name, opts, &block)
    end

  end
  
end
