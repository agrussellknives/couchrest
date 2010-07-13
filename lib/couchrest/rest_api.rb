module RestAPI

  def put(uri, doc = nil)
    payload = doc.to_json if doc
    begin
      JSON.parse(RestClient.put(uri, payload))
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
      JSON.parse(RestClient.get(uri), :max_nesting => false)
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
      JSON.parse(RestClient.post(uri, payload))
    rescue Exception => e
      if $DEBUG
        raise "Error while sending a POST request #{uri}\npayload: #{payload.inspect}\n#{e}"
      else
        raise e
      end
    end
  end

  def delete(uri)
    JSON.parse(RestClient.delete(uri))
  end

  def copy(uri, destination) 
    JSON.parse(RestClient::Request.execute( :method => :copy,
                                            :url => uri,
                                            :headers => {'Destination' => destination}
                                          ).to_s)
  end 

end
