package cloud.phusion.dev;

import cloud.phusion.DataObject;

public interface IntegrationMocker {

    DataObject executeIntegration(String integrationId, DataObject msg) throws Exception;

}
