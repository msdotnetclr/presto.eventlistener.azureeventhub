package presto.EventListener.AzureEventHub;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AzureEventHubListener implements EventListener {

    private static final Logger log = Logger.get(AzureEventHubListener.class);

    private EventHubClient ehClient = null;


    public AzureEventHubListener(Map<String, String> requiredConfig) {
        final String namespaceName = requireNonNull(requiredConfig.get("azure-event-hub.namespace-name"), "azure-event-hub.namespace-name is null");
        final String eventHubName = requireNonNull(requiredConfig.get("azure-event-hub.event-hub-name"), "azure-event-hub.event-hub-name is null");
        final String sasKeyName = requireNonNull(requiredConfig.get("azure-event-hub.sas-key-name"), "azure-event-hub.sas-key-name is null");
        final String sasKey = requireNonNull(requiredConfig.get("azure-event-hub.sas-key"), "azure-event-hub.sas-key is null");

        ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);

        try {
            ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());
        }
        catch(EventHubException ex){
            log.error(ex);
        }
        catch(IOException ex){
            log.error(ex);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        Gson obj = new GsonBuilder().disableHtmlEscaping().create();
        String json = obj.toJson(queryCreatedEvent);
        EventData sendEvent = new EventData(json.getBytes());
        try {
            ehClient.sendSync(sendEvent);
        } catch (Exception e) {
            log.error("Error sending QueryCreatedEvent to Azure EventHub. ErrorMessage: %s", e.getMessage());
            log.error("EventHub write failed: %s", json);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        Gson obj = new GsonBuilder().disableHtmlEscaping().create();
        String json = obj.toJson(queryCompletedEvent);
        EventData sendEvent = new EventData(json.getBytes());
        try {
            ehClient.sendSync(sendEvent);
        } catch (Exception e) {
            log.error("Error sending QueryCompletedEvent to Azure EventHub. ErrorMessage: %s", e.getMessage());
            log.error("EventHub write failed: %s", json);
        }
    }
}
