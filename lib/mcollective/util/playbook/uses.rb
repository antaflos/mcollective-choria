module MCollective
  module Util
    class Playbook
      class Uses
        def initialize(playbook)
          @playbook = playbook
          @_uses = {}
        end

        def [](agent)
          @_uses[agent]
        end

        def keys
          @_uses.keys
        end

        # Validates agent versions on nodes
        #
        # @param agents [Hash] a hash of agent names and nodes that uses that agent
        # @raize [StandardError] on failure
        def validate_agents(agents)
          nodes = agents.map{|_, nodes| nodes}.flatten.uniq

          Log.info("Validating agent inventory on %d nodes" % nodes.size)

          rpc = Tasks::RpcTask.new
          rpc.from_hash(
            "nodes" => nodes,
            "action" => "rpcutil.agent_inventory",
            "silent" => true
          )
          success, msg, inventory = rpc.run

          validation_fail = false

          if success
            agents.each do |agent, nodes|
              unless @_uses.include?(agent)
                Log.error("Agent %s is mentioned in node sets but not declared in the uses list" % agent)
                validation_fail = true
                next
              end

              nodes.each do |node|
                if node_inventory = inventory.find{|i| i[:sender] == node}
                  if metadata = node_inventory[:data][:agents].find{|i| i[:agent] == agent}
                    if valid_version?(metadata[:version], @_uses[agent])
                      Log.debug("Agent %s on %s version %s matches desired version %s" % [agent, node, metadata[:version], @_uses[agent]])
                    else
                      Log.error("Agent %s on %s version %s does not match desired version %s" % [agent, node, metadata[:version], @_uses[agent]])
                      validation_fail = true
                    end
                  else
                    Log.error("Node %s does not have the agent %s" % [node, agent])
                    validation_fail = true
                  end
                else
                  Log.error("Did not receive an inventory for node %s" % node)
                  validation_fail = true
                end
              end
            end
          else
            raise("Could not determine agent inventory: %s" % msg)
          end

          raise("Network agents did not match specified SemVer specifications in the playbook") if validation_fail

          Log.info("Agent inventory on %d nodes validated" % nodes.size)
        end

        # Determines if a semver version is within a stated range
        #
        # @note mcollective never suggested semver, so versions like "1.1" becomes "1.1.0" for the compare
        # @param have [String] SemVer of what you have
        # @param want [String] SemVer range of what you need
        # @return [Boolean]
        # @raise [StandardError] on invalid version strings
        def valid_version?(have, want)
          if have.split(".").size == 2
            have = "%s.0" % have
          end

          semver_have = SemanticPuppet::Version.parse(have)
          semver_want = SemanticPuppet::VersionRange.parse(want)
          semver_want.include?(semver_have)
        end

        # Checks that all the declared agent DDLs exist
        #
        # @raise [StandardError] on invalid DDLs
        def prepare
          update_report

          invalid = @_uses.map do |agent, want|
            begin
              have = ddl_version(agent)

              if valid_version?(have, want)
                Log.debug("Agent %s DDL version %s matches desired %s" % [agent, have, want])
                nil
              else
                Log.warn("Agent %s DDL version %s does not match desired %s" % [agent, have, want])
                agent
              end
            rescue
              Log.warn("Could not process DDL for agent %s: %s: %s" % [agent, $!.class, $!.to_s])
              agent
            end
          end.compact

          raise("DDLs for agent(s) %s did not match desired versions" % invalid.join(", ")) unless invalid.empty?
        end

        def update_report
          keys.each do |agent|
            @playbook.report.record_use(agent, self[agent])
          end
        end

        # Fetches the DDL version for an agent
        #
        # If the agent DDL has versions like 1.0 it will be
        # turned into 1.0.0 as old mco stuff didnt do semver
        #
        # @param agent [String]
        def ddl_version(agent)
          ddl = agent_ddl(agent)
          ddl.meta[:version]

        end

        # Returns the DDL for a specific agent
        #
        # @param agent [String]
        # @return [DDL::AgentDDL]
        # @raise [StandardError] should the DDL not exist
        def agent_ddl(agent)
          DDL::AgentDDL.new(agent)
        end

        def from_hash(data)
          data.each do |agent, version|
            Log.debug("Loading usage of %s version %s" % [agent, version])
            @_uses[agent] = version
          end

          self
        end
      end
    end
  end
end
