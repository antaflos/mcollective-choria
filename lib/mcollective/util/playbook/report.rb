module MCollective
  module Util
    class Playbook
      class Report
        def initialize(playbook)
          @playbook = playbook
          @report = {
            "version" => "1",
            "metadata" => {},
            "times" => {
              "start_time" => Time.now.utc.to_i,
              "end_time" => nil
            },
            "uses" => {},
            "inputs" => {},
            "nodes" => {},
            "rpc_results" => [],
            "outcome" => {
              "success" => false,
              "message" => "Unknown failure, report not finalized"
            }
          }
        end

        def to_hash
          @report.clone
        end

        def finalize(result="OK")
          @report["times"]["end_time"] = Time.now.utc.to_i
          @report["outcome"] = {
            "success" => result == "OK",
            "message" => result
          }
        end

        def record_rpc_result(result)
          @report["rpc_results"] << result
        end

        def record_use(agent, version)
          @report["uses"][agent] = version
        end

        def record_metadata(metadata)
          @report["metadata"] = metadata.clone
        end

        def record_nodeset(set, nodes)
          @report["nodes"][set] = nodes
        end

        def record_input(input, value)
          @report["inputs"][input] = value
        end
      end
    end
  end
end
