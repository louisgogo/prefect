-- Monthly business-line staging batch versioning.
-- Review and apply in a transaction before deploying batch-aware flow code.
-- Existing rows receive a BLS-YYYYMM-000 baseline batch lazily on the first flow run.

CREATE TABLE IF NOT EXISTS public.bus_line_staging_batch (
    batch_id UUID PRIMARY KEY,
    batch_no VARCHAR(30) NOT NULL UNIQUE,
    acct_period DATE NOT NULL,
    version_no INTEGER NOT NULL,
    previous_batch_id UUID REFERENCES public.bus_line_staging_batch(batch_id),
    status VARCHAR(20) NOT NULL,
    flow_run_id VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ready_at TIMESTAMP,
    activated_at TIMESTAMP,
    published_at TIMESTAMP,
    failed_at TIMESTAMP,
    error_message TEXT,
    UNIQUE (acct_period, version_no)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_flow_run
ON public.bus_line_staging_batch(flow_run_id)
WHERE flow_run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_filling_period
ON public.bus_line_staging_batch(acct_period)
WHERE status = 'FILLING';

CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_published_period
ON public.bus_line_staging_batch(acct_period)
WHERE status = 'PUBLISHED';

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'staging_bus_expense',
        'staging_bus_revenue',
        'staging_bus_profit_bd',
        'staging_bus_inventory',
        'staging_bus_receivable',
        'staging_bus_in_transit_inventory'
    ]
    LOOP
        IF to_regclass('public.' || table_name) IS NOT NULL THEN
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS batch_id UUID', table_name);
            EXECUTE format(
                'CREATE UNIQUE INDEX IF NOT EXISTS %I ON public.%I(batch_id, "来源编号", "唯一层级") WHERE batch_id IS NOT NULL',
                'uq_' || table_name || '_batch_source_lvl',
                table_name
            );
        END IF;
    END LOOP;
END $$;
