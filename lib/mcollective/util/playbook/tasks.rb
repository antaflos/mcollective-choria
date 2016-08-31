require_relative "tasks/rpc_task"

module MCollective
  module Util
    class Playbook
      class Tasks
        include TemplateUtil

        def initialize(playbook)
          @playbook = playbook
          @_tasks = {
            "tasks" => [],
            "pre_task" => [],
            "post_task" => [],
            "on_fail" => [],
            "on_success" => [],
            "pre_book" => [],
            "post_book" => []
          }
        end

        def prepare
        end

        def runner_for(type)
          Tasks::RpcTask.new
        end

        # Runs a specific task
        #
        # @param task [Hash] a task entry
        # @param hooks [Boolean] indicates if hooks should be run
        # @return [Boolean] indicating task success
        def run_task(task, hooks=true)
          properties = task[:properties]
          success = false

          if properties["description"]
            Log.info("About to run task: %s" % properties["description"])
          end

          if hooks && !run_set("pre_task")
            Log.error("Failing task because a critical pre_task hook failed")
            return false
          end

          (1..properties["tries"]).each do |try|
            task[:runner].from_hash(t(properties))
            task[:runner].validate_configuration!

            success, msg, replies = task[:runner].run

            if task[:type] == "rpc"
              replies.each do |reply|
                @playbook.record_rpc_result(reply)
              end
            end

            Log.info(msg)

            if properties["fail_ok"] && !success
              Log.warn("Task failed but fail_ok is true, treating as success")
              success = true
            end

            if try != properties["tries"] && !success
              Log.warn("Task failed on try %d/%d, sleeping %ds: %s" % [try, properties["tries"], properties["try_sleep"], msg])
              sleep(properties["try_sleep"])
            end

            break if success
          end

          if hooks && !run_set("post_task")
            Log.error("Failing task because a critical post_task hook failed")
            return false
          end

          success
        end

        # Runs a specific task set
        #
        # @param set [String] one of the known task sets
        # @return [Boolean] true if all tasks and all their hooks passed
        def run_set(set)
          set_tasks = @_tasks[set]

          return true if set_tasks.empty?

          __enter_context = @playbook.context
          @playbook.context = set

          Log.info("About to run task set %s with %d task(s)" % [set, set_tasks.size])

          set_success = set_tasks.map do |task|
            @playbook.context = "%s.%s" % [set, task[:type]]
            run_task(task, set == "tasks")
            @playbook.context = set
          end.all?

          Log.info("Done running task set %s with %d task(s): success: %s" % [set, set_tasks.size, set_success])

          @playbook.context = __enter_context

          set_success
        end

        def run
          @context = "running"

          unless run_set("pre_book")
            Log.error("Playbook pre_book hook failed to run, failing entire playbook")
            return false
          end

          success = run_set("tasks")

          Log.info("Finished running main tasks in playbook: success: %s" % success)

          if success
            sucess = run_set("on_success")
          else
            run_set("on_fail")
          end

          unless run_set("post_book")
            Log.error("Playbook post_book hook failed to run, failing entire playbookbook")
            return false
          end

          @playbook.context = ""

          success
        end

        def load_tasks(data, set)
          data.each_with_index do |task, idx|
            task.each do |type, props|
              Log.debug("Loading task %d of type %s" % [idx, type])

              runner = runner_for(type)

              @_tasks[set] << {
                :type => type,
                :properties => {
                  "tries" => 1,
                  "try_sleep" => 10,
                  "fail_ok" => false
                }.merge(props),
                :runner => runner
              }
            end
          end
        end

        def from_hash(data)
          if data.is_a?(Array)
            load_tasks(data, "tasks")
          else
            data.each do |set, tasks|
              load_tasks(tasks, set)
            end
          end

          self
        end
      end
    end
  end
end
