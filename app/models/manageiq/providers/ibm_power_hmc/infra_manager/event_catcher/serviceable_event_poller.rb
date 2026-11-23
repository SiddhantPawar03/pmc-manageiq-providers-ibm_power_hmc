require 'json'
require_relative '../utility/xml_to_json_transformer'

# Encapsulates the periodic polling of IBM HMC Serviceable Events.
#
# Called from Stream#poll on each loop iteration; responsible for:
#   1. Deciding whether the configured poll interval has elapsed.
#   2. Fetching raw XML from the HMC via +connection.serviceable_events_search+.
#   3. Parsing each feed entry and creating (EmsEvent.add_queue) or updating
#      the matching EmsEvent record.
#
class ManageIQ::Providers::IbmPowerHmc::InfraManager::EventCatcher::ServiceableEventPoller
  POLL_INTERVAL = 600 # seconds between successive serviceable-event fetches

  def initialize(ems)
    @ems       = ems
    @last_poll = Time.now.utc.to_i - POLL_INTERVAL
  end

  # Poll serviceable events if the interval has elapsed.
  #
  # @param connection [IbmPowerHmc::Connection]  active HMC connection from Stream#poll
  def poll(connection)
    now = Time.now.utc.to_i
    return unless now >= @last_poll + POLL_INTERVAL

    @last_poll = now
    fetch_and_process(connection)
  end

  private

  def fetch_and_process(connection)
    sem_xml = connection.serviceable_events_search
    return if sem_xml.blank?

    feed = ManageIQ::Providers::IbmPowerHmc::InfraManager::XmlToJsonTransformer.transform(sem_xml)
    process_entries(feed)
  end

  def process_entries(feed)
    entries = feed.dig("feed", "entries") || []
    return if entries.empty?

    # ── ONE bulk SELECT for the entire batch ──────────────────────────────────
    # Fetch id + message + full_data for all already-persisted ServiceableEvents
    # for this EMS.  The key is the composite "prob_uuid_problem_state" (message).
    # For each message we keep the id of the row and the canonical string form of
    # its stored full_data, so an upstream change can be detected by a plain
    # string comparison.  Rows are read in id order, so when old duplicates share
    # a message the newest row wins.
    # message => {:id => Integer, :data => String}
    existing_keys = {}
    EmsEvent.where(:ems_id => @ems.id, :event_type => "ServiceableEvent", :source => "IBM_POWER_HMC")
            .order(:id)
            .pluck(:id, :message, :full_data)
            .each { |id, msg, data| existing_keys[msg] = {:id => id, :data => canonical_string(data)} }

    # A failure on one entry must not prevent the remaining entries from being
    # processed in this poll.
    entries.each do |entry|
      upsert_entry(entry, existing_keys)
    rescue => err
      $ibm_power_hmc_log.error("[ServiceableEvents] entry id=#{entry["id"]} failed: #{err.class}: #{err.message}")
    end
  end

  # Process a single feed entry using the pre-fetched key map.
  #   - message not found            → create a new record (queued)
  #   - found, full_data identical   → nothing to do
  #   - found, full_data differs     → update the existing record in place
  def upsert_entry(entry, existing_keys)
    sem       = entry.dig("content", "ServiceableEvent") || {}
    entry_id  = entry["id"]
    published = entry["published"]

    # ── Mapped columns ────────────────────────────────────────────────────────
    # message   → Problem UUID + "_" + Problem State (e.g. d8d65290-..._OPEN)
    # host_name → Failing Console MTMS  (machtype-model*serial)
    # vm_name   → Partition Name
    prob_uuid     = extract_value(sem["problemUuid"])
    problem_state = extract_value(sem["problemState"])

    # Without a problem UUID the message key would be just "_<state>"
    if prob_uuid.blank?
      $ibm_power_hmc_log.warn("[ServiceableEvents] skipping entry id=#{entry_id}: problemUuid is missing")
      return
    end

    failing_mtms = build_failing_mtms(sem)
    lpar_name    = extract_value(sem["partitionName"])

    sem_data = {
      :problem_uuid              => prob_uuid,
      :problem_number            => sem.dig("problemNumber", "_value"),
      :problem_state             => problem_state,
      :event_severity            => sem.dig("eventSeverity", "_value"),
      :reference_code            => sem.dig("referenceCode", "_value"),
      :reference_code_extension  => extract_value(sem["referenceCodeExtension"]),
      :system_reference_code     => extract_value(sem["systemReferenceCode"]),
      :notification_type         => sem.dig("notificationType", "_value"),
      :notification_status       => extract_value(sem["notificationStatus"]),
      :symptom_string            => sem.dig("symptomString", "_value"),
      :short_description         => sem.dig("shortDescription", "_value"),
      :duplicate_count           => sem.dig("duplicateCount", "_value"),
      :platform_log_id           => sem.dig("platformLogId", "_value"),
      :approval_state            => extract_value(sem["ApprovalState"]),
      :call_home_enabled         => extract_value(sem["callHomeEnabled"]),
      :post_action               => extract_value(sem["postAction"]),
      :hw_record                 => extract_value(sem["problemManagementHardwareRecord"]),
      :total_events              => extract_value(sem["totalEvents"]),
      :syslog_id                 => extract_value(sem["sysLogId"]),
      :primary_timestamp         => extract_value(sem["primaryTimestamp"]),
      :created_timestamp         => extract_value(sem["createdTimestamp"]),
      :first_reported_timestamp  => extract_value(sem["firstReportedTimestamp"]),
      :last_reported_timestamp   => extract_value(sem["lastReportedTimestamp"]),
      :failing_mtms              => failing_mtms,
      :reporting_console_host    => sem.dig("reportingConsoleNode", "hostName"),
      :partition_id              => extract_value(sem["partitionId"]),
      :partition_name            => lpar_name,
      :partition_hostname        => extract_value(sem["partitionHostName"]),
      :partition_os_type         => extract_value(sem["partitionOSType"]),
      :frus                      => sem.dig("fieldReplaceableUnits", "FieldReplaceableUnit"),
      :src_extension_data        => sem["srcExtnData"],
      :extended_error_files      => sem.dig("extendedErrorData", "ExtendedFileData"),
      :service_history           => sem.dig("serviceHistoryData", "ServiceHistory")
    }

    event_hash = {
      :event_type => "ServiceableEvent",
      :source     => "IBM_POWER_HMC",
      :ems_ref    => entry_id,
      :timestamp  => published,
      :message    => "#{prob_uuid}_#{problem_state}",
      :host_name  => failing_mtms,
      :vm_name    => lpar_name,
      :full_data  => sem_data,
      :ems_id     => @ems.id
    }

    message_key = "#{prob_uuid}_#{problem_state}"
    new_data    = canonical_string(sem_data)
    existing    = existing_keys[message_key]

    if existing.nil?
      # No record for this message yet → create it.
      EmsEvent.add_queue('add', @ems.id, event_hash)
      existing_keys[message_key] = {:id => nil, :data => new_data}
    elsif existing[:data] != new_data
      # Record exists but upstream data changed → update it in place.
      update_existing(existing, event_hash, new_data)
    end
  end

  # Update the stored row for an existing message with the latest upstream values.
  # A queued-but-not-yet-persisted row (:id nil) cannot be updated; it already
  # carries the data that was queued earlier in this poll.
  def update_existing(existing, event_hash, new_data)
    return if existing[:id].nil?

    EmsEvent.find(existing[:id]).update!(
      :full_data => event_hash[:full_data],
      :host_name => event_hash[:host_name],
      :vm_name   => event_hash[:vm_name]
    )
    existing[:data] = new_data
    $ibm_power_hmc_log.info("[ServiceableEvents] updated event_id=#{existing[:id]} message=#{event_hash[:message]}")
  rescue => err
    $ibm_power_hmc_log.error("[ServiceableEvents] update failed event_id=#{existing[:id]} #{err.class}: #{err.message}")
  end

  # Canonical string form of a value, used to compare upstream data with the
  # stored full_data.
  def canonical_string(value)
    JSON.generate(canonicalize(value))
  end

  def canonicalize(value)
    case value
    when Hash  then value.map { |k, v| [k.to_s, canonicalize(v)] }.sort_by(&:first).to_h
    when Array then value.map { |v| canonicalize(v) }
    else value
    end
  end

  # Extract the plain string value from a transformer node.
  # Plain leaf elements are already a String (or nil).
  def extract_value(node)
    node.kind_of?(Hash) ? node["_value"] : node
  end

  # Build the Failing Console MTMS string from the transformer-produced SEM hash.
  # The transformer converts the XML directly to nested string-keyed hashes, so
  # failingManagedSystemNode/managedTypeModelSerial/{MachineType,Model,SerialNumber}
  # are already present — we just concatenate them the same way the SDK does.
  # Returns nil when the node is absent entirely.
  def build_failing_mtms(sem)
    node     = sem.dig("failingManagedSystemNode", "managedTypeModelSerial") || {}
    machtype = extract_value(node["MachineType"])
    model    = extract_value(node["Model"])
    serial   = extract_value(node["SerialNumber"])
    return nil if machtype.nil? && model.nil? && serial.nil?

    "#{machtype}-#{model}*#{serial}"
  end
end