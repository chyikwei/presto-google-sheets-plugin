package com.chyikwei.presto.google.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class SheetsModule
        implements Module
{
    private final TypeManager typeManager;

    public SheetsModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(SheetsConnector.class).in(Scopes.SINGLETON);
        binder.bind(SheetsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SheetsClient.class).in(Scopes.SINGLETON);
        binder.bind(SheetsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SheetsRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SheetsConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(SheetsTable.class));
    }

//    @Provides
//    @Singleton
//    public SheetsCredentialsSupplier provideSheetsCredentialsSupplier(SheetsConfig config)
//    {
//        return new SheetsCredentialsSupplier(config.getCredentialsFilePath());
//    }
//
//    @Provides
//    @Singleton
//    public SheetsApiClient provideSheetsApiClient(SheetsConfig config, SheetsCredentialsSupplier credentialsSupplier)
//    {
//        return new SheetsApiClient(credentialsSupplier);
//    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
