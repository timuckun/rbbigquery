module RbBigQuery
  class GoogleApi


    def initialize(opts={})
      #these will be injected into this object as attr_accessor
      HashParams.new(opts, self) do
        param :application_name, :required => true
        param :application_version, :required => true
        param :key_path, :required => true
        param :service_email, :required => true
        param :cache_timeout_in_seconds, coerse: Integer, :default => 60 * 60
        param :cache_directory, default: '/tmp'
      end


      @raw_client = Google::APIClient.new(
          application_name:    application_name,
          application_version: application_version
      )

      key = Google::APIClient::PKCS12.load_key(File.open(@key_path, mode: 'rb'), 'notasecret')

      asserter = Google::APIClient::JWTAsserter.new(
          @service_email,
          'https://www.googleapis.com/auth/bigquery',
          key
      )

      @raw_client.authorization = asserter.authorize


    end


    def discover(api, version, cache_timeout=nil)


      cache_timeout ||= @cache_timeout_in_seconds
      filename      = File.join(@cache_directory, "discovered_google_api_#{api}_#{version}.json")

      cache_valid     = File.exists?(filename) && (Time.now - File.ctime(filename)) < cache_timeout
      cached_document = File.read(filename) if cache_valid
      #   this will register a previously discovered document with the client
      #    it eliminates a needless http request
      @raw_client.register_discovery_document(api, @ersion, cached_document) if cached_document
      #   #this call will only initiatiate an http response if the discovery document is missing
      discovered_api = @raw_client.discovered_api(api, version)
      #
      #   #if there was no cached document write one.
      File.write(filename, discovered_api.discovery_document.to_json) unless cached_document

      # return the discovered_api
      discovered_api
    end

    #delegated methods to @raw_client
    def execute(*params)
      @raw_client.execute(*params)
    end
  end
end

