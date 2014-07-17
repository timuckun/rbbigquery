module RbBigQuery
  class GoogleApi
    attr :errors

    def initialize(opts={})
      @errors = []
      check_param opts, :application_name
      check_param opts, :application_version
      check_param opts, :key_path
      check_param opts, :service_email
      check_param opts, :env, :default => ENV['RBBIGQUERY_ENV'] || ENV['RACK_ENV'] || ENV['RAILS_ENV'] || 'development'

      @client = Google::APIClient.new(
          application_name:    opts[:application_name],
          application_version: opts[:application_version]
      )

      #authorize
      key     = Google::APIClient::PKCS12.load_key(File.open(opts[:key_path], mode: 'rb'), 'notasecret')

      asserter = Google::APIClient::JWTAsserter.new(
          opts[:service_email],
          'https://www.googleapis.com/auth/bigquery',
          key
      )

      @client.authorization = asserter.authorize

    end


    def check_param(opts, key, h = {})
      opts[key] ||= opts[key.to_s] || h[:default]
      @errors << "Parameter #{key} is required and missing" unless opts[key]
      opts[key]

    end


    def discover_api(api, version, cached_api_file_name = nil, cache_timeout_in_seconds = nil)

      # TODO: Make this a setting or a constant
      cache_timeout_in_seconds ||= 60 * 60
      cached_api_file_name     ||= "/tmp/discovered_google_api_#{api}_#{version}.json"

      cached_document          = File.read(cached_api_file_name) if File.exists?(cached_api_file_name) && (Time.now - File.ctime(cached_api_file_name)) < cache_timeout_in_seconds

      # this will register a previously discovered document with the client
      # it eliminates a needless http request
      @client.register_discovery_document(api, version, cached_document) if cached_document
      #this call will only initiatiate an http response if the discovery document is missing
      discovered_api = @client.discovered_api(api, version)

      #if there was no cached document write one.
      File.write(cached_api_file_name, discovered_api.discovery_document.to_json) unless cached_document
      discovered_api
    end


  end

  def client

  end

end


# @params opts [Hash] {:application_name, :application_version, :key_path, :service_email, :project_id}
def initialize(opts = {})

  @project_id = opts[:project_id]
  @env        = opts[:env] || ENV['RBBIGQUERY_ENV'] || ENV['RACK_ENV'] || ENV['RAILS_ENV'] || 'development'


end

# @return [RbBigQuery::Table]
def find_or_create_table(dataset, table_id, schema)
  RbBigQuery::Table.new(self, dataset, table_id, schema)
end

# Executes provided query.
# @param [String] query
# @return [String] row response string
def query(query)
  response = execute bq.jobs.query, {}, {'query' => query}
  build_rows_from_response(response)
end

# Generic API call
def execute(api_method, parameters={}, body_object ={}, opts={})
  parameters['projectId'] ||= parameters.delete(:project_id) || parameters.delete('project_id') || @project_id

  h               = {:api_method => api_method, :parameters => camelize_hash_keys(parameters)}
  h[:body_object] = camelize_hash_keys(body_object) unless body_object.empty?
  h.merge.opts unless opts.empty?
  @client.execute h
end

def datasets(params={})
  response = execute(bq.datasets.list, params)
  body     = JSON.parse(response.body)
  body['datasets'].map { |h| h['datasetReference']['datasetId'] }
end

private


def camelize_hash_keys(h={})
  h.inject({}) { |h2, (k, v)| h2[camel_case_lower(k)]=v; h2 }
end

def camel_case_lower(s)
  #first word always lower case
  s.to_s.split('_').inject([]) { |buffer, e| buffer.push(buffer.empty? ? e.downcase : e.capitalize) }.join
end


end