module RbBigQuery
  # Schema Builder
  class Schema
    attr_accessor :schema

    class << self
      # Builds schema for BigQuery
      # @param &blk [Proc] RbBigQuery schema DSL
      # @return [Array<Hash>]
      def build(&blk)
        instance        = new
        instance.schema = []
        instance.instance_eval &blk
        instance.schema
      end
    end

    def string(name)
      self.schema.push({
                           type: 'STRING',
                           name: name
                       })
    end

    def integer(name)
      self.schema.push({
                           type: 'INTEGER',
                           name: name
                       })
    end

    def float(name)
      self.schema.push({
                           type: 'FLOAT',
                           name: name
                       })
    end

    def boolean(name)
      self.schema.push({
                           type: 'BOOLEAN',
                           name: name
                       })
    end

    def timestamp(name)
      self.schema.push({
                           type: 'TIMESTAMP',
                           name: name
                       })
    end

    alias_method :time_stamp, :timestamp

    def record(name)
      self.schema.push({
                           type: 'RECORD',
                           name: name
                       })
    end
  end
end
