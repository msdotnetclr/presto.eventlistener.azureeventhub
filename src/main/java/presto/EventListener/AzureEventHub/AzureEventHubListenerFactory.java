package presto.EventListener.AzureEventHub;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;


public class AzureEventHubListenerFactory implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "azure-event-hub";
    }

    @Override
    public EventListener create(Map<String, String> requiredConfig)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        return new AzureEventHubListener(requiredConfig);
    }
}
