class ManageIQ::Providers::IbmPowerHmc::InfraManager::EventCatcher::Stream
  def initialize(ems, _options = {})
    @ems                = ems
    @stop_polling       = false
    @serviceable_poller = serviceable_event_poller
  end

  def start
    @stop_polling = false
  end

  def stop
    @stop_polling = true
  end

  def poll(&block)
    @ems.with_provider_connection do |connection|
      until @stop_polling
        poll_uom_events(connection, &block)
        poll_serviceable_events(connection)
      end
    end
  end

  private

  def poll_uom_events(connection, &block)
    connection.next_events(false)
              .select { |event| event.type.in?(%w[ADD_URI MODIFY_URI DELETE_URI]) }
              .each(&block)
  rescue IbmPowerHmc::Connection::HttpError => err
    $ibm_power_hmc_log.error("querying hmc events failed: #{err}")
  rescue => err
    $ibm_power_hmc_log.error("#{err.class}: #{err.message}")
  end

  def poll_serviceable_events(connection)
    @serviceable_poller.poll(connection)
  rescue => err
    $ibm_power_hmc_log.error("serviceable events poll failed: #{err.class}: #{err.message}")
  end

  def serviceable_event_poller
    self.class.module_parent::ServiceableEventPoller.new(@ems)
  end
end
