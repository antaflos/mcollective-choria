module MCollective
  module Util
    class Playbook
      class Inputs
        def initialize(playbook)
          @playbook = playbook
          @_inputs = {}
        end

        def keys
          @_inputs.keys
        end

        # Creates options for each input in the application
        #
        # @param application [MCollective::Application]
        # @param set_required [Boolean] when true required inputs will be required on the CLI
        def add_cli_options(application, set_required)
          klass = application.class

          @_inputs.each do |input, props|
            i_props = props[:properties]

            type = case i_props["type"]
                   when :string, "String"
                     String
                   when :fixnum, "Fixnum", "Integer"
                     Integer
                   else
                     i_props["type"]
                   end

            description = "%s (%s) %s" % [
              i_props["description"],
              type,
              i_props["default"] ? ("default: %s" % i_props["default"]) : ""
            ]

            option_params = {
              :description => description,
              :arguments => ["--%s %s" % [input.downcase, input.upcase]],
              :type => type,
              :default => i_props["default"],
              :validation => i_props["validation"]
            }

            option_params[:required] = i_props["required"] if set_required

            klass.option(input, option_params)
          end
        end

        # Attempts to find values for all declared inputs
        #
        # @param data [Hash] input data
        # @return [void]
        # @raise [StandardError] when required data could not be found
        # @raise [StandardError] when validation fails for any data
        def prepare(data={})
          @_inputs.each do |input, props|
            if data.include?(input)
              validate_data(input, data[input])
              @_inputs[input][:value] = data[input]
            end
          end

          validate_requirements

          keys.each do |input|
            @playbook.report.record_input(input, self[input])
          end
        end

        def include?(input)
          @_inputs.include?(input)
        end

        # Retrieves the value for a specific input
        #
        # @param input [String] input name
        # @return [Object]
        # @raise [StandardError] for unknown inputs
        def [](input)
          if include?(input)
            @_inputs[input][:value]
          else
            raise("Unknown input %s" % input)
          end
        end

        # Checks all required inputs have values
        #
        # @raise [StandardError] when not
        def validate_requirements
          invalid = @_inputs.map do |input, props|
            if props[:properties]["required"]
              if !props[:value]
                Log.warn("Input %s requires a value but has none or nil" % input)
                input
              end
            end
          end.compact

          raise("Values were required but not given for inputs: %s" % invalid.join(", ")) unless invalid.empty?
        end

        # Validates a piece of data against an input
        #
        # @param input [String] a valid input name
        # @param value [Object] a value to validate
        # @raise [StandardError] on validation failure
        def validate_data(input, value)
          validator = @_inputs[input][:properties]["validation"]

          if validator =~ /^:(.+)/
            validator = $1.intern
          elsif validator =~ /^\/(.+)\/$/
            validator = Regexp.new($1)
          end

          Log.debug("Validating input %s using %s validator" % [input, validator])

          Validator.validate(value, validator)
        rescue
          raise("Failed to validate value for input %s: %s" % [input, $!.to_s])
        end

        def from_hash(data)
          data.each do |input, props|
            props["required"] = true unless props.include?("required")

            Log.debug("Loading input %s" % [input])
            @_inputs[input] = {
              :properties => props,
              :value => props.include?("default") ? props["default"] : nil
            }
          end

          self
        end
        end
    end
  end
end
