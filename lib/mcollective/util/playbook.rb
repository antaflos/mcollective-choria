require_relative "playbook/playbook_logger"
require_relative "playbook/template_util"
require_relative "playbook/inputs"
require_relative "playbook/uses"
require_relative "playbook/nodes"
require_relative "playbook/tasks"
require_relative "playbook/report"

require "semantic_puppet"
require "json-schema"

module MCollective
  module Util
    class Playbook
      include TemplateUtil

      attr_accessor :context
      attr_reader :loglevel, :report

      def initialize(loglevel=nil)
        @loglevel = loglevel

        @report = Report.new(self)
        @nodes = Nodes.new(self)
        @tasks = Tasks.new(self)
        @uses = Uses.new(self)
        @inputs = Inputs.new(self)
        @metadata = {
          "name" => nil,
          "version" => nil,
          "author" => nil,
          "description" => nil,
          "tags" => [],
          "on_fail" => "fail",
          "loglevel" => "info",
          "run_as" => "choria=deployer"
        }

        @logger = Log.set_logger(Playbook_Logger.new(self))

        @playbook = self
        @playbook_data = {}
        @input_data = {}
      end

      def name
        @metadata["name"]
      end

      def loglevel
        @loglevel || @metadata["loglevel"] || "info"
      end

      def set_logger_level
        @logger.set_level(loglevel.intern)
      end

      def prepare
        # do this first for templating down below
        @context = "inputs.prep"
        @inputs.prepare(@input_data)

        @context = "uses.prep"
        @uses.from_hash(t(@playbook_data.fetch("uses", {}))).prepare
        @context = "nodes.prep"
        @nodes.from_hash(t(@playbook_data.fetch("nodes", {}))).prepare

        # we lazy template parse these so that they might refer to run time
        # state via the template system - like for example in a post task you
        # might want to reference properties of another rpc request
        @context = "tasks.prep"
        @tasks.from_hash(@playbook_data.fetch("tasks", []))
        @tasks.from_hash(@playbook_data.fetch("hooks", {})).prepare
      end

      def record_rpc_result(result)
        @report.record_rpc_result(result)
      end

      # Runs the playbook
      #
      # @param inputs [Hash] input data
      # @param verbose [Boolean] to log verbosely
      # @return [Boolean]
      def run!(inputs)
        start_time = Time.now

        @input_data = inputs

        prepare

        @context = "run"
        @tasks.run

        @report.finalize

        Log.info("Done running playbook %s in %s" % [@metadata["name"], seconds_to_human(Integer(Time.now - start_time))])
      rescue
        Log.error("Playbook %s failed: %s: %s" % [@metadata["name"], $!.class, $!.to_s])
        Log.debug($!.backtrace.join("\n\t"))
        @report.finalize("Failed: %s: %s" % [$!.class, $!.to_s])
        false
      end

      def seconds_to_human(seconds)
        days = seconds / 86400
        seconds -= 86400 * days

        hours = seconds / 3600
        seconds -= 3600 * hours

        minutes = seconds / 60
        seconds -= 60 * minutes

        if days > 1
          "%d days %d hours %d minutes %02d seconds" % [days, hours, minutes, seconds]
        elsif days == 1
          "%d day %d hours %d minutes %02d seconds" % [days, hours, minutes, seconds]
        elsif hours > 0
          "%d hours %d minutes %02d seconds" % [hours, minutes, seconds]
        elsif minutes > 0
          "%d minutes %02d seconds" % [minutes, seconds]
        else
          "%02d seconds" % seconds
        end
      end

      # Validates agent versions on nodes
      #
      # @param agents [Hash] a hash of agent names and nodes that uses that agent
      # @raize [StandardError] on failure
      def validate_agents(agents)
        @uses.validate_agents(agents)
      end

      def metadata_item(item)
        if @metadata.include?(item)
          @metadata["item"]
        else
          raise("Unknown playbook metadata %s" % item)
        end
      end

      def discovered_nodes(nodeset)
        if @nodes.include?(nodeset)
          @nodes[nodeset]
        else
          raise("Unknown nodeset %s" % nodeset)
        end
      end

      def input_value(input)
        if @inputs.include?(input)
          @inputs[input]
        else
          raise("Unknown input %s" % input)
        end
      end

      def add_cli_options(application, set_required=false)
        @inputs.add_cli_options(application, set_required)
      end

      def from_hash(data)
        @context = "loading"
        @playbook_data = data
        @metadata = {
          "name" => @playbook_data["name"],
          "version" => @playbook_data["version"],
          "author" => @playbook_data["author"],
          "description" => @playbook_data["description"],
          "tags" => @playbook_data.fetch("tags", []),
          "on_fail" => @playbook_data.fetch("on_fail", "fail"),
          "loglevel" => @playbook_data.fetch("loglevel", "info"),
          "run_as" => @playbook_data["run_as"]
        }

        @report.record_metadata(@metadata)

        @@name = @playbook_data["name"]

        set_logger_level

        @context = "inputs"
        @inputs.from_hash(@playbook_data.fetch("inputs", {}))
        @context = ""

        self
      end
    end
  end
end
