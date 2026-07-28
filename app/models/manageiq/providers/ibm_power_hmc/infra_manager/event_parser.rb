require "base64"

module ManageIQ::Providers::IbmPowerHmc::InfraManager::EventParser
  def self.event_to_hash(event, ems_id)
    full_data = {
      "data"     => event.data,
      "detail"   => event.detail,
      "usertask" => event.usertask
    }

    event_hash = {
      :event_type => event.type,
      :source     => 'IBM_POWER_HMC',
      :ems_ref    => event.id,
      :timestamp  => event.published,
      :message    => event.detail,
      :full_data  => Base64.strict_encode64(full_data.to_json),
      :ems_id     => ems_id
    }

    elems = URI(event.data).path.split('/')
    type, uuid = elems[-2], elems[-1]

    # Check if the URI also contains /ManagedSystem/{uuid}/
    host_uuid = elems[-3] if elems.length >= 4 && elems[-4] == "ManagedSystem"

    case type
    when "ManagedSystem"
      event_hash[:host_ems_ref] = uuid
    when "LogicalPartition", "VirtualIOServer"
      event_hash[:vm_ems_ref]   = uuid
      event_hash[:host_ems_ref] = host_uuid unless host_uuid.nil?
    when "VirtualSwitch", "VirtualNetwork", "SharedProcessorPool", "SharedMemoryPool"
      event_hash[:host_ems_ref] = host_uuid unless host_uuid.nil?
    when "UserTask"
      event_hash[:message] = event.usertask["key"]
    end

    event_hash
  end

  # Build an event_hash for a single Serviceable Event (SEM) feed entry.
  #
  # @param entry  [Hash]    one element from feed["feed"]["entries"] as produced
  #                         by XmlToJsonTransformer — the full entry is stored in
  #                         :full_data so every field is available downstream.
  # @param ems_id [Integer] id of the owning ExtManagementSystem record
  # @return [Hash]          event_hash ready to pass to EmsEvent.add
  def self.sem_entry_to_hash(entry, ems_id)
    sem      = entry.dig("content", "ServiceableEvent") || {}
    prob_num = sem.dig("problemNumber", "_value")
    entry_id = entry["id"]
    published = entry["published"]

    {
      :event_type => "ServiceableEvent",
      :source     => "IBM_POWER_HMC",
      :ems_ref    => entry_id,
      :timestamp  => published,
      :message    => "ServiceableEvent problemNumber=#{prob_num}",
      # Store the entire entry as Base64-encoded JSON.
      # Read back: JSON.parse(Base64.decode64(ems_event.full_data)).dig("content", "ServiceableEvent", "problemNumber", "_value")
      :full_data  => Base64.strict_encode64(entry.to_json),
      :ems_id     => ems_id
    }
  end
end
