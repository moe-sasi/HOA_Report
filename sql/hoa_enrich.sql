USE WholeLoan;

IF OBJECT_ID('tempdb..#tape_loan_ids') IS NULL
BEGIN
    THROW 50000, '#tape_loan_ids temp table is required for hoa_enrich.sql', 1;
END;

SELECT
    CAST(qrm.RWT_LOAN_NUMBER AS NVARCHAR(100)) AS loan_id,
    ltd.LoanNum,
    '' AS 'SEMT ID',
    qrm.COMMITMENT_NUM AS 'Bulk ID',
    COALESCE(
        CAST(ltd.MERSLoanID AS NVARCHAR),
        CAST(lrn.RefNum AS NVARCHAR),
        CAST(qrm.LS_LOAN_TRAN_ID AS NVARCHAR),
        CAST(qrm.LS_LOAN_APP_ID AS NVARCHAR),
        CAST(MERS_MINIdentifier AS NVARCHAR)
    ) AS [MERS Number],
    qrm.LENDER_NAME AS 'Seller',
    qrm.LENDER_LOAN_NUMBER AS 'Collateral ID',
    '' AS 'Alternate ID',
    'SPS' AS 'Primary Servicer',
    COALESCE(ltd.ServicerLoanNumber, w.svcCurrentLoanNo) AS 'Servicer Loan Number',
    w.settledate AS 'RWT Purchase Date',
    COALESCE(ltd.propertyAddress, AddressLineText) AS 'Property Address',
    w.propertyCity AS 'Property City',
    w.propertystate AS 'Property State',
    w.propertyZipCode AS 'Property Zip',
    '' AS 'HOA',
    '' AS 'HOA Monthly Payment',
    '' AS 'Securitized Balance',
    '' AS 'Securitized Next Due Date',
    ltd.DueDiligenceVendor,
    ltd.[SubLoanReviewType]
FROM loanData.reporting.v_qrm qrm
INNER JOIN #tape_loan_ids tape_ids ON tape_ids.loan_id = CAST(qrm.RWT_LOAN_NUMBER AS NVARCHAR(100))
LEFT JOIN loanData.reporting.clayton_v_1_3_2 c132 ON c132.RWTLOANNO = qrm.RWT_LOAN_NUMBER
LEFT JOIN wholeloans w ON w.rwtLoanNo = qrm.LOANNUM
LEFT JOIN [LM_LINKED].LM_PLYBSE.dbo.V_LOAN_TRUE_DATA ltd ON qrm.LOANNUM = ltd.LoanNum
LEFT JOIN [REPL_LS_LOS].dbo.[LoanAppLoc1] lal ON lal.LoanNum = c132.RWTLOANNO
LEFT JOIN [REPL_LS_LOS].dbo.[LoanRefNumLoc1] lrn ON lal.LoanTranId = lrn.LoanTranId AND lrn.RefTypCd = 'MERS'
ORDER BY ltd.LoanNum ASC;
