def generate_ms_dim_query(reg_code: str,
                          invoice_number: str):
    
    """
    Get financial dimensions associated with XML based on seller reg code and invoice number
    """

    return \
        f"""
    select
    gjae.TEXT,
    gjae.LEDGERACCOUNT,
    gjae.TRANSACTIONCURRENCYAMOUNT,
    gjae.ISCORRECTION,
    ma.NAME,
    ma.MAINACCOUNTID,
    gjae.LEDGERDIMENSION,
    davc.OSAKOND,
    davc.OSAKONDVALUE,
    dft_osak.DESCRIPTION as "DepartmentName",
    davc.POHIVARA,
    davc.POHIVARAVALUE,
    at_pvara.NAME as "MainAssetName",
    davc.KULUKOHT,
    davc.KULUKOHTVALUE,
    dacc_kkeskus.NAME as "CostCenterName",
    davc.PROJEKT,
    davc.PROJEKTVALUE,
    dft_projekt.DESCRIPTION as "ProjectName",
    davc.LEPING,
    davc.LEPINGVALUE,
    evr_ct.RCONTRACTSUBJECT as "ContractDesc"
from
    (
    -- get credit account entries from general journal
    select *
    from dbo.GENERALJOURNALACCOUNTENTRY
    where dbo.GENERALJOURNALACCOUNTENTRY.ISCREDIT in (0,1) and
        dbo.GENERALJOURNALACCOUNTENTRY.GENERALJOURNALENTRY in (
        -- get general journal entry based on voucher number
        select dbo.GENERALJOURNALENTRY.RECID as "GENERALJOURNALENTRY"
        from dbo.GENERALJOURNALENTRY
        where dbo.GENERALJOURNALENTRY.SUBLEDGERVOUCHER in (
            -- get voucher by invoice number and seller reg number
            select dbo.DCEINVOICETABLE.VOUCHER
            from dbo.DCEINVOICETABLE
            where dbo.DCEINVOICETABLE.INVOICENUM = '{invoice_number}'
            and dbo.DCEINVOICETABLE.SELLERREGNUMBER = '{reg_code}'
            and dbo.DCEINVOICETABLE.VOUCHER != ''))) as gjae
-- join financial dimensions
left join dbo.DIMENSIONATTRIBUTEVALUECOMBINATION as davc on
    davc.RECID = gjae.LEDGERDIMENSION
-- join account
left join dbo.MAINACCOUNT as ma on
    ma.RECID = gjae.MAINACCOUNT
-- dim name value texts for "Osakond"
left join dbo.DimensionFinancialTag as dft_osak on
    dft_osak.RECID = davc.OSAKOND
-- dim name value for "Leping"
left join dbo.EVRCONTRACTTABLE as evr_ct on
    evr_ct.RECID = davc.LEPING
-- dim name value for "Project"
left join dbo.DimensionFinancialTag as dft_projekt on
    dft_projekt.RECID = davc.PROJEKT
-- dim name value for "Kulukeskus"
left join dbo.DIMATTRIBUTEOMCOSTCENTER as dacc_kkeskus on
    dacc_kkeskus.RECID = davc.KULUKOHT
-- dim name value for "Põhivara"
left join dbo.ASSETTABLE as at_pvara on
    at_pvara.RECID = davc.POHIVARA;
    """