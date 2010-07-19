module RestAPI

  def default_headers
    {
      :content_type => :json,
      :accept       => :json
    }
  end

  def put(uri, doc = nil)
    payload = doc.to_json if doc
    begin
      JSON.parse(RestClient.put(uri, payload, default_headers))
    rescue Exception => e
      if $DEBUG
        raise "Error while sending a PUT request #{uri}\npayload: #{payload.inspect}\n#{e}"
      else
        raise e
      end
    end
  end

  def get_raw(uri)
    begin
      res = RestClient.get(uri)
      rjson = JSON.parse(res, :max_nesting => false)
      rj_class = class << rjson; self; end
      rj_class.send(:define_method, :raw_response) do
        res
      end
      rjson
    rescue JSON::ParserError
      # not our responsibility.
      return res
    rescue => e
      if $DEBUG
        raise "Error while sending a GET request #{uri}\n: #{e}"
      else
        raise e
      end
    end
  end
  
  def get(uri)
    begin
      JSON.parse(RestClient.get(uri, default_headers), :max_nesting => false)
    rescue => e
      if $DEBUG
        raise "Error while sending a GET request #{uri}\n: #{e}"
      else
        raise e
      end
    end
  end

  def post(uri, doc = nil)
    payload = doc.to_json if doc
    begin
      JSON.parse(RestClient.post(uri, payload, default_headers))
    rescue Exception => e
      if $DEBUG
        raise "Error while sending a POST request #{uri}\npayload: #{payload.inspect}\n#{e}"
      else
        raise e
      end
    end
  end

  def delete(uri)
    JSON.parse(RestClient.delete(uri, default_headers))
  end

  def copy(uri, destination) 
    JSON.parse(RestClient::Request.execute( :method => :copy,
                                            :url => uri,
                                            :headers => default_headers.merge('Destination' => destination)
                                          ).to_s)
  end 

end
