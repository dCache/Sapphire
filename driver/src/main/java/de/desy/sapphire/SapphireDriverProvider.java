package de.desy.sapphire;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class SapphireDriverProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "sapphire";
    }

    @Override
    public String getDescription()
    {
        return "Combines small files to bigger files for archiving on tape";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new SapphireDriver(type, name);
    }
}
