module RbBigQuery
  class Client
    attr_accessor :client, :project_id, :bq, :api, :version


    # @params opts [Hash] {:application_name, :application_version, :key_path, :service_email, :project_id}
    def initialize(opts = {})
      HashParams.new(opts, self) do
        param :project_id
        param :env, default: ENV['RBBIGQUERY_ENV'] || ENV['RACK_ENV'] || ENV['RAILS_ENV'] || 'development'
        param :api, default: 'bigquery'
        param :version, default: 'v2'
      end

      @client=RbBigQuery::GoogleApi.new(opts)

      @bq = @client.discover(@api, @version)

    end

    # @return [RbBigQuery::Table]
    def find_or_create_table(dataset, table_id, schema)
      RbBigQuery::Table.new(self, dataset, table_id, schema)
    end

    # Executes provided query.
    # @param query [String] query
    # @return [Array]  An array of hashes
    def query(query)
      response = execute bq.jobs.query, {}, {'query' => query}
      build_rows_from_response(response)
    end

    # Executes provided query and returns the first value of the first row as a scalar value.
    # @param query [String] query
    # @param cast [String|Symbol] method to send to the result (optionsl)
    # @return Scalar value with casting if requested
    def select_value(query, cast = nil)
      r=query(query)[0]['f0_']
      r.send(cast) if cast && r.respond_to?(cast)
    end

    # Generic API call
    def execute(api_method, parameters={}, body_object ={}, opts={})
      parameters['projectId'] ||= parameters.delete(:project_id) || parameters.delete('project_id') || @project_id
      h                       = {:api_method => api_method, :parameters => parameters}
      h[:body_object]         = body_object unless body_object.empty?
      h.merge.opts unless opts.empty?
      @client.execute h
    end

    def datasets(params={})
      response = execute(bq.datasets.list, params)
      body     = JSON.parse(response.body)
      body['datasets'].map { |h| h['datasetReference']['datasetId'] }
    end

    private


    # def camelize_hash_keys(h={})
    #   h.inject({}) { |h2, (k, v)| h2[camel_case_lower(k)]=v; h2 }
    # end
    #
    # def camel_case_lower(s)
    #   #first word always lower case
    #   s.to_s.split('_').inject([]) { |buffer, e| buffer.push(buffer.empty? ? e.downcase : e.capitalize) }.join
    # end


    # Sample response
    #
    #{"kind"=>"bigquery#queryResponse",
    #"schema"=>
    #    {"fields"=>
    #         [{"name"=>"screen_name", "type"=>"STRING", "mode"=>"NULLABLE"},
    #          {"name"=>"text", "type"=>"STRING", "mode"=>"NULLABLE"}]},
    #    "jobReference"=>
    #    {"projectId"=>"#{SOME_PROJECTID}", "jobId"=>"#{SOME_JOBID}"},
    #    "totalRows"=>"15",
    #    "rows"=>
    #    [{"f"=>[{"v"=>"huga"}, {"v"=>"text: 5"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 2"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 4"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 3"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 3"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 1"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 1"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 4"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 2"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 5"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 5"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 3"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 1"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 4"}]},
    #     {"f"=>[{"v"=>"huga"}, {"v"=>"text: 2"}]}],
    #    "totalBytesProcessed"=>"225",
    #    "jobComplete"=>true,
    #    "cacheHit"=>false
    #}
    # @return [Array<Hash>]
    def build_rows_from_response(response)
      return unless response
      body   = JSON.parse(response.body)
      schema = body["schema"]["fields"]

      body["rows"].map do |row|
        row_hash = {}
        row["f"].each_with_index do |field, index|
          name           = schema[index]["name"]
          row_hash[name] = field["v"]
        end
        row_hash
      end
    end

  end
end