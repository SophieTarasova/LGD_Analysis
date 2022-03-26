----create table
with payment_d as (
     select 
            contract.id, 
            last_value(pay.date_instalment) over (partition by contract.id order by pay.date_instalment
                                         rows between unbounded preceding and unbounded following) as last_pymnt_d
     from contract_data contract 
     join payment_data pay        on contract.id = pay.id and contract.date_approval = pay.date_approval 
     where contract.date_approval >= 20210101 
           and pay.date_approval >= 20210101 
     group by contract.id), 
last_bureau as (
      select 
                  contract.id, 
                  last_value(bureau.credit_pull_d) over (partition by contract.id_client order by pay.date_decision
                                               rows between unbounded preceding and unbounded following) as last_credit_pull_d, 
                  
                  last_value(bureau.fico_range_high) over (partition by contract.id_client order by pay.date_decision
                                               rows between unbounded preceding and unbounded following) as last_fico_range_high, 
                  
                  last_value(bureau.fico_range_low) over (partition by contract.id_client order by pay.date_decision
                                               rows between unbounded preceding and unbounded following) as last_fico_range_low
           from contract_data contract 
           join bureau_info bureau        on contract.id_client = bureau.id_client
           where contract.date_approval >= 20210101 
           group by contract.id)

select --+parallel(8)
       app.id, 
       app.loan_amnt, 
       contract.funded_amnt, 
       contract.funded_amnt_inv, 
       contract.term, 
       contract.interest_rate, 
       contract.installment, 
       app.grade, 
       app.sub_grade, 
       app_add.emp_title,
       app_add.emp_length, 
       app.home_ownership, 
       app.annual_inc, 
       contract.verification_status, 
       contract.issue_d, 
       contract.loan_status, 
       contact.pymnt_plan, 
       chan_d.url, 
       app.purpose, 
       app.title, 
       app.zip_code, 
       app.addr_state, 
       bureau.dti, 
       bureau.delinq_2yrs, 
       bureau.earliest_cr_line, 
       bureau.fico_range_low, 
       bureau.fico_range_high, 
       bureau.inq_last_6mths, 
       bureau.mths_since_last_delinq, 
       bureau.mths_since_last_record, 
       bureau.open_acc, 
       bureau.pub_rec, 
       bureau.revol_bal, 
       bureau.revol_util, 
       bureau.total_acc, 
       app.initial_list_status, 
       contract.out_prncp, 
       contract.out_prncp_inv, 
       contract.total_pymnt, 
       contract.total_pymnt_inv, 
       contract.total_rec_prncp, 
       contract.total_rec_int, 
       contract.total_rec_late_fee, 
       contract.recoveries, 
       contract.collection_recovery_fee, 
       p.last_pymnt_d, 
       pay.pymnt_amnt as last_pymnt_amnt, 
       pay.next_pymnt_d, 
       bureau_last.last_credit_pull_d, 
       bureau_last.last_fico_range_high, 
       last_bureau.last_fico_range_low, 
       coll.collections_12_mths_ex_med, 
       contract.mths_since_last_major_derog, 
       app.policy_code, 
       app.application_type, 
       app.annual_inc_joint, 
       bureau.dti_joint, 
       contract.verification_status_joint, 
       contract.acc_now_delinq, 
       contract.tot_coll_amt, 
       contract.tot_cur_bal, 
       bureau.open_acc_6m, 
       bureau.open_act_il, 
       bureau.open_il_12m, 
       bureau.open_il_24m, 
       contract.mths_since_rcnt_il, 
       contract.total_bal_il, 
       contract.il_util, 
       bureau.open_rv_12m, 
       bureau.open_rv_24m, 
       bureau.max_bal_bc, 
       cl.all_util, 
       bureau.total_rev_hi_lim, 
       contract.inq_fi, 
       contract.total_cu_tl,
       contract.inq_last_12m, 
       cl.acc_open_past_24mths, 
       cl.avg_cur_bal, 
       contract.bc_open_to_buy, 
       contract.bc_util, 
       contract.chargeoff_within_12_mths, 
       coll.delinq_amnt, 
       cl.mo_sin_old_il_acct, 
       cl.mo_sin_old_rev_tl_op, 
       cl.mo_sin_rcnt_rev_tl_op, 
       cl.mo_sin_rcnt_tl, 
       cl.mort_acc, 
       cl.mths_since_recent_bc, 
       cl.mths_since_recent_bc_dlq, 
       cl.mths_since_recent_inq, 
       cl.mths_since_recent_revol_delinq, 
       cl.num_accts_ever_120_pd, 
       cl.num_actv_bc_tl, 
       cl.num_actv_rev_tl, 
       cl.num_bc_sats, 
       cl.num_bc_tl, 
       cl.num_il_tl, 
       cl.num_op_rev_tl, 
       cl.num_rev_accts, 
       cl.num_rev_tl_bal_gt_0, 
       cl.num_sats, 
       cl.num_tl_120dpd_2m, 
       cl.num_tl_30dpd, 
       cl.num_tl_90g_dpd_24m, 
       cl.num_tl_op_past_12m, 
       cl.pct_tl_nvr_dlq, 
       cl.percent_bc_gt_75, 
       cl.pub_rec_bankruptcies, 
       cl.tax_liens, 
       cl.tot_hi_cred_lim, 
       cl.total_bal_ex_mort, 
       contract.total_bc_limit, 
       contract.total_il_high_credit_limit, 
       cl.revol_bal_joint, 
       cl.sec_app_fico_range_low,
       cl.sec_app_fico_range_high, 
       cl.sec_app_earliest_cr_line, 
       cl.sec_app_inq_last_6mths, 
       cl.sec_app_mort_acc, 
       cl.sec_app_open_acc, 
       cl.sec_app_revol_util, 
       cl.sec_app_open_act_il, 
       cl.sec_app_num_rev_accts, 
       cl.sec_app_chargeoff_within_12_mths, 
       cl.sec_app_collections_12_mths_ex_med, 
       case when hard.is is not null then 1 else end hardship_flag, 
       hard.hardship_type, 
       hard.hardship_reason, 
       hard.hardship_status, 
       hard.deferral_term, 
       hard.hardship_amount, 
       hard.hardship_start_date, 
       hard.hardship_end_date, 
       hard.payment_plan_start_date, 
       hard.hardship_length, 
       hard.hardship_dpd, 
       hard.hardship_loan_status, 
       hard.orig_projected_additional_accrued_interest, 
       hard.hardship_payoff_balance_amount, 
       hard.hardship_last_payment_amount, 
       hard.debt_settlement_flag
    
       
from application_data app
join contract_data contract                                on app.id = contract.id and app.date_decision = contract.date_approval
join application_add_data app_add                          on app.id = app_add.id and app.date_decision = app_add.date_decision
join application_channel chan                              on app.id = chan.id and app.date_decision = chan.date_decision
join channel_dict chan_d                                   on chan.channel_id = chand_d.channel_id
join bureau_info bureau                                    on app.id = bureau.id and app.date_decision = bureau.date_decision
join payment_d p                                           on contract.id = p.id 
join payment_data pay                                      on contract.id = pay.id and pay.date_instalment = p.last_pymnt_d
join last_bureau bureau_last                               on contract.id = bureau_last.id 
left join collection_data coll                             on on coll.id = contract.id and coll.date_decision = contract.date_approval
join client cl                                             on contract.client_id = cl.client_id
left join hardship_info hard                               on contract.id = hard.id and hard.date_approval = contract.date_approval

where app.date_decision >= 20210101
      and contract.date_approval >= 20210101 
      and bureau.date_decision >= 20210101
      and app.application_status = 'Approved'
      and app_add.address_type = 'Employer'
;



----   
      
    
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
