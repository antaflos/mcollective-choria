require_relative "nodes/mcollective_nodes"

module MCollective
  module Util
    class Playbook
      class Nodes
        def initialize(playbook)
          @playbook = playbook
          @_nodes = {}
        end

        def keys
          @_nodes.keys
        end

        def [](nodes)
          if include?(nodes)
            @_nodes[nodes][:discovered]
          else
            raise("Unknown node set %s" % nodes)
          end
        end

        def properties(nodes)
          @_nodes[nodes][:properties]
        end

        def include?(nodes)
          @_nodes.include?(nodes)
        end

        def update_report
          keys.each do |set|
            @playbook.report.record_nodeset(set, self[set])
          end
        end

        def prepare
          @_nodes.each do |nodes, dets|
            @playbook.context = nodes

            Log.debug("Preparing nodeset %s" % nodes)
            dets[:resolver].prepare
            dets[:discovered] = dets[:resolver].discover

            check_empty(nodes)
            limit_nodes(nodes)
            update_report
            validate_nodes(nodes)

            Log.info("Discovered %d node(s) in node set %s" % [dets[:discovered].size, nodes])
          end

          @playbook.context = "conn.test"
          test_nodes
          @playbook.context = "ddl.test"
          check_uses
        end

        # Checks if the agents on the nodes matches the desired versions
        #
        # @raise [StandardError] on error
        def check_uses
          agent_nodes = {}

          @_nodes.map do |nodes, dets|
            dets[:properties].fetch("uses", []).each do |agent|
              agent_nodes[agent] ||= []
              agent_nodes[agent].concat(dets[:discovered])
            end
          end

          @playbook.validate_agents(agent_nodes)
        end

        # Determines if a nodeset needs connectivity test
        #
        # @param nodes [String] node set name
        # @return [Boolean]
        def should_test?(nodes)
          !!properties(nodes)["test"]
        end

        # Tests a RPC ping to the discovered nodes
        #
        # @todo is this really needed?
        # @raise [StandardError] on error
        def test_nodes
          nodes_to_test = []

          @_nodes.each do |nodes, dets|
            nodes_to_test.concat(self[nodes]) if should_test?(nodes)
          end

          return if nodes_to_test.empty?

          Log.info("Checking connectivity for %d nodes" % nodes_to_test.size)

          rpc = Tasks::RpcTask.new
          rpc.from_hash(
            "nodes" => nodes_to_test,
            "action" => "rpcutil.ping",
            "silent" => true
          )
          success, msg, _ = rpc.run

          unless success
            raise("Connectivity test failed for some nodes: %s" % [msg])
          end
        end

        # Checks that discovered nodes matches stated expectations
        #
        # @param nodes [String] node set name
        # @raise [StandardError] on error
        def validate_nodes(nodes)
          unless self[nodes].size >= properties(nodes)["at_least"]
            raise("Node set %s needs at least %d nodes, got %d" % [nodes, properties(nodes)["at_least"], self[nodes].size])
          end
        end

        # Handles an empty discovered list
        #
        # @param nodes [String] node set name
        # @raise [StandardError] when empty
        def check_empty(nodes)
          if self[nodes].empty?
            if reason = properties(nodes)["when_empty"]
              raise(reason)
            else
              raise("Did not discover any nodes for nodeset %s" % nodes)
            end
          end
        end

        # Limits the discovered list for a node set based on the playbook limits
        #
        # @param nodes [String] node set name
        def limit_nodes(nodes)
          if limit = properties(nodes)["limit"]
            Log.debug("Limiting node set %s to %d nodes from %d" % [nodes, limit, @_nodes[nodes][:discovered].size])
            @_nodes[nodes][:discovered] = @_nodes[nodes][:discovered][0..(limit - 1)]
          end
        end

        def resolver_for(type)
          Nodes::McollectiveNodes.new
        end

        def from_hash(data)
          data.each do |nodes, props|
            resolver = resolver_for(props["type"])
            resolver.from_hash(props)
            resolver.validate_configuration!

            @_nodes[nodes] = {
              :properties => props,
              :resolver => resolver,
              :discovered => []
            }
          end

          self
        end
        end
    end
  end
end
