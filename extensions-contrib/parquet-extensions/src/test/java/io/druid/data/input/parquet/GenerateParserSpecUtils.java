package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.data.input.parquet.model.Field;
import io.druid.data.input.parquet.model.FieldType;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregator;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class GenerateParserSpecUtils {

    @Test
    public void testGenerateFieldsFromString() {
//        String input = "[\"timestamp\",\"experiencedexperiments\",\"experimentnames\",\"experimentversions\",\"experiencedtreatments\",\"treatmentnames\",\"treatmentversions\",\"eventdate\",\"attributesmap[isp_name]\",\"attributesmap[isp_user_type]\",\"attributesmap[language_id]\",\"attributesmap[locale]\",\"attributesmap[component_name]\",\"attributesmap[page_name]\",\"attributesmap[page_group]\",\"attributesmap[os]\",\"attributesmap[browser_version]\",\"attributesmap[browser_type]\",\"attributesmap[decr_account_number]\",\"attributesmap[decr_merchant_account_number]\",\"attributesmap[decr_transaction_id]\",\"attributesmap[device_id]\",\"attributesmap[device]\",\"attributesmap[device_model]\",\"attributesmap[ip]\",\"attributesmap[connection_type]\",\"attributesmap[mobile_device_name]\",\"attributesmap[user_logged_in]\",\"attributesmap[visitor_id]\",\"attributesmap[cookie_id]\",\"attributesmap[bot]\",\"attributesmap[enrich]\",\"attributesmap[country_iso_code]\",\"attributesmap[country_name]\",\"attributesmap[continent]\",\"attributesmap[sw]\",\"attributesmap[screen_height]\",\"attributesmap[device_height]\",\"attributesmap[device_width]\",\"attributesmap[event_type]\",\"attributesmap[event_identifier]\",\"cld_attributes[prmry_card_added_dt]\",\"cld_attributes[business_type]\",\"cld_attributes[dc_exp_dt_year]\",\"cld_attributes[emailable_status_y_n]\",\"cld_attributes[cust_intent]\",\"cld_attributes[is_debit_card]\",\"cld_attributes[last_received_dispute_dt]\",\"cld_attributes[wc_page_last_visit_dt]\",\"cld_attributes[is_student_acct_y_n]\",\"cld_attributes[cust_age_group_code]\",\"cld_attributes[acct_locked_dt]\",\"cld_attributes[is_guest_y_n]\",\"cld_attributes[first_recv_pmt_txn_dt]\",\"cld_attributes[mobile_app_dwnld_y_n]\",\"cld_attributes[first_cb_recv_dt]\",\"cld_attributes[acct_rstrd_dt]\",\"cld_attributes[email_spam_policy_change_y_n]\",\"cld_attributes[cust_busn_seg_key]\",\"cld_attributes[sub_indy_id]\",\"cld_attributes[guest_wax_acct_upgrde_dt]\",\"cld_attributes[busn_name]\",\"cld_attributes[last_ach_confirm_dt]\",\"cld_attributes[dc_exp_dt_mth]\",\"cld_attributes[yob]\",\"cld_attributes[busn_addr_state]\",\"cld_attributes[frs_optin_dt]\",\"cld_attributes[prmry_card_removed_y_n]\",\"cld_attributes[prmer_to_busn_upgrde_dt]\",\"cld_attributes[cip_status]\",\"cld_attributes[mkt_sub_regn_key]\",\"cld_attributes[mobile_acqstn_chnl_desc]\",\"cld_attributes[first_pmt_sent_recv_flag]\",\"cld_attributes[dpc_y_n]\",\"cld_attributes[last_email_open_dt]\",\"cld_attributes[last_received_chargeback_dt]\",\"cld_attributes[merc_website_url]\",\"cld_attributes[account_type]\",\"cld_attributes[prmry_email_vrfd_dt]\",\"cld_attributes[busn_addr_zip_code]\",\"cld_attributes[cust_actv_seg_key]\",\"cld_attributes[prmry_card_auth_credit_dt]\",\"cld_attributes[prmry_reside_cntry_code]\",\"cld_attributes[retrv_forgot_pwd_dt]\",\"cld_attributes[first_sent_pmt_txn_dt]\",\"cld_attributes[pan_verified_y_n]\",\"cld_attributes[mobile_app_login_y_n]\",\"cld_attributes[last_card_added_dt]\",\"cld_attributes[wc_loan_tkn_dt]\",\"cld_attributes[wc_decln_dt]\",\"cld_attributes[last_pmt_txn_dt]\",\"cld_attributes[prmry_ach_added_dt]\",\"cld_attributes[last_web_access_dt]\",\"cld_attributes[last_recv_pmt_txn_dt]\",\"cld_attributes[frs_y_n]\",\"cld_attributes[wc_whitelist_mbr_y_n]\",\"cld_attributes[ppme_slug_active_y_n]\",\"cld_attributes[last_card_dcline_dt]\",\"cld_attributes[last_sent_pmt_txn_dt]\",\"cld_attributes[ppme_attr_type_val]\",\"cld_attributes[email_spam_dev_ntwrk_y_n]\",\"cld_attributes[merch_cntry_code]\",\"cld_attributes[prmry_addr_state]\",\"cld_attributes[busn_to_prsnl_dwngrde_dt]\",\"cld_attributes[acct_rstrd_y_n]\",\"cld_attributes[ppme_page_last_visit_dt]\",\"cld_attributes[last_ach_added_dt]\",\"cld_attributes[prmry_ach_removed_dt]\",\"cld_attributes[last_email_delivered_dt]\",\"cld_attributes[busn_addr_cntry_code]\",\"cld_attributes[prmry_email_addr]\",\"cld_attributes[cust_engagmnt_seg_key]\",\"cld_attributes[last_pmt_sent_recv_flag]\",\"cld_attributes[prmry_addr_city]\",\"cld_attributes[acct_cre_dt]\",\"cld_attributes[gender]\",\"cld_attributes[prmry_ach_vrfd_dt]\",\"cld_attributes[wc_applied_dt]\",\"cld_attributes[cust_fin_rptg_cntry_code]\",\"cld_attributes[is_credit_card]\",\"cld_attributes[email_spam_promos_y_n]\",\"cld_attributes[pp_here_terminal_y_n]\",\"cld_attributes[indy_id]\",\"cld_attributes[mobile_num]\",\"cld_attributes[email_spam_newsletter_y_n]\",\"cld_attributes[last_ach_decline_dt]\",\"cld_attributes[prmry_card_expired_y_n]\",\"cld_attributes[prsnl_to_prmr_upgrde_dt]\",\"cld_attributes[first_ach_decline_dt]\",\"cld_attributes[last_email_click_dt]\",\"cld_attributes[tot_dispute_received]\",\"cld_attributes[first_pmt_txn_dt]\",\"cld_attributes[email_bnce_dt]\",\"cld_attributes[pphere_association_name]\",\"cld_attributes[last_card_dcline_rsn]\",\"cld_attributes[wc_loan_amt_curr_code]\",\"cld_attributes[first_fraud_reversal_recv_dt]\",\"cld_attributes[pph_signup_dt]\",\"cld_attributes[is_large_merchant_y_n]\",\"cld_attributes[pp_here_actvtn_dt]\",\"cld_attributes[acct_clsd_dt]\",\"cld_attributes[prmry_ach_auth_deposit_dt]\",\"cld_attributes[last_card_auth_verify_dt]\",\"cld_attributes[last_app_login_dt]\",\"cld_attributes[merch_min_acct_y_n]\",\"cld_attributes[ppme_slug_cre_dt]\",\"cld_attributes[wc_eligible_amt_curr_code]\",\"cld_attributes[open_wax_acct_y_n]\",\"cld_attributes[email_confirmed_y_n]\",\"cld_attributes[last_topup_dt]\",\"cld_attributes[prmry_card_vrfd_dt]\",\"cld_attributes[true_indy_name]\",\"cld_attributes[is_account_managed_y_n]\",\"cld_attributes[wc_actv_loan_y_n]\",\"cld_attributes[wc_repaid_amt_curr_code]\",\"cld_attributes[cust_first_name]\",\"cld_attributes[ever_ach_vrfd_y_n]\",\"cld_attributes[is_le_merchant_y_n]\",\"cld_attributes[kyc_status]\",\"cld_attributes[wc_hist_loan_count]\",\"cld_attributes[cust_curr_code]\",\"cld_attributes[acct_locked_y_n]\",\"cld_attributes[busn_to_prmer_dwngrde_dt]\",\"cld_attributes[last_fraud_rvsl_recv_dt]\",\"cld_attributes[merc_website_flg_proxy]\",\"cld_attributes[amex_card_y_n]\",\"cld_attributes[email_spam_slr_tips_y_n]\",\"cld_attributes[prmry_addr_zip_code]\",\"cld_attributes[cust_acct_clsfn_key]\",\"cld_attributes[wc_approved_dt]\",\"cld_attributes[wc_loan_repaid_dt]\",\"cld_attributes[glb_suppression_y_n]\",\"cld_attributes[email_spam_dt]\",\"cld_attributes[cust_lang_code]\",\"cld_attributes[prmry_email_domain]\",\"cld_attributes[is_multi_currency_bal_y_n]\",\"cld_attributes[casual_seller_y_n]\",\"cld_attributes[first_odr_recv_case_dt]\",\"cld_attributes[busn_addr_city]\",\"cld_attributes[cust_last_name]\",\"cld_attributes[balance_effective_dt]\",\"cld_attributes[IN_KYC_COMPLETE_Y_N]\",\"cld_attributes[acct_clsd_y_n]\",\"cld_attributes[mkt_regn_code]\",\"cld_attributes[last_received_claim_dt]\",\"cld_attributes[prmr_to_prsnl_dwngrde_dt]\",\"cld_attributes[wc_eligible_amt]\",\"cld_attributes[last_pmt_txn_usd_amt]\",\"cld_attributes[first_sent_pmt_txn_amt]\",\"cld_attributes[party_key]\",\"cld_attributes[wc_repaid_amt]\",\"cld_attributes[actv_conf_bank_accts_cnt]\",\"cld_attributes[first_sent_pmt_txn_usd_amt]\",\"cld_attributes[tot_bal_local_currency]\",\"cld_attributes[tot_topup_last12m]\",\"cld_attributes[ntpv_12_mth_sent_usd_amt]\",\"cld_attributes[last_sent_pmt_txn_amt]\",\"cld_attributes[last_sent_pmt_txn_usd_amt]\",\"cld_attributes[tot_recv_pmt_txn_cnt]\",\"cld_attributes[tot_bal_equiv_usd_amt]\",\"cld_attributes[wc_loan_amt]\",\"cld_attributes[actv_cc_on_file_cnt]\",\"cld_attributes[tot_claims_received]\",\"cld_attributes[tot_recv_pmt_txn_amt]\",\"cld_attributes[tot_send_pmt_txn_cnt]\",\"cld_attributes[tot_chargebacks_received]\",\"cld_attributes[last_recv_pmt_txn_amt]\",\"cld_attributes[first_recv_pmt_txn_usd_amt]\",\"cld_attributes[ntpv_12_mth_sent_cnt]\",\"cld_attributes[last_recv_pmt_txn_usd_amt]\",\"cld_attributes[actv_bank_accts_on_file_cnt]\",\"cld_attributes[actv_conf_cc_on_file_cnt]\",\"cld_attributes[tot_rev_usd_amt]\",\"cld_attributes[tot_non_topup_last12m]\",\"cld_attributes[tot_send_pmt_txn_amt]\",\"cld_attributes[tot_rev_recv_usd_amt]\",\"cld_attributes[first_pmt_txn_usd_amt]\",\"cld_attributes[tot_rev_send_usd_amt]\"]";
        String input = "[\"visitor_id\", \"cust_id\", \"geo_cntry\", \"ip_addr\", \"browser_type\", \"browser_ver\", \"client_os\", \"mobile_os\", \"mobile_app_version\", \"devc_type_name\", \"mobile_devc\", \"bot_flag\", \"devc_name\", \"prspct_chnl\", \"is_raw_prspct_y_n\", \"is_qfd_prspct_y_n\", \"qfd_prspct_type_code\", \"is_signup_start_y_n\", \"ss_prspct_type_code\", \"is_signup_cmplt_y_n\", \"sc_prspct_type_code\", \"is_upgrde_cmplt_y_n\", \"upgrde_evnt_type\", \"acqstn_type_code\", \"is_repeat_prospect_indicator\", \"cust_fin_rptg_cntry_code\", \"cust_acct_type_code\", \"cust_signup_dt\", \"is_upsell_start_y_n\", \"ups_prspct_type_code\", \"is_upsell_cmplt_y_n\", \"ups_cmplt_prspct_type_code\", \"is_tot_visitor_y_n\", \"is_upgrade_start_y_n\", \"upg_start_prspct_type_code\", \"is_spanning\", \"prspct_chnl_prev_day\", \"eu_page_title\", \"eu_page_grp_name\", \"eu_page_name\", \"eu_page_lnk\", \"referrer_domain\", \"scndry_prty_cookie_id\", \"rosetta_lang_cd\", \"mobile_device_id\", \"referring_url\", \"evnt_page_url\", \"cnt_pv_prspct\", \"cnt_sessn_prspct_pv\", \"cnt_sessn_signup_start_pv\", \"tfs_category\", \"channel_pros_tag\", \"channel_flow\", \"event_date\"]";
        List<String> fields = JsonUtils.readFrom(input, List.class);
        List<io.druid.data.input.parquet.model.Field> parsedFields = io.druid.data.input.parquet.model.Field.parseFields(fields);
        List<Field> rs = new ArrayList<Field>();
        for (io.druid.data.input.parquet.model.Field field : io.druid.data.input.parquet.model.Field.parseFields(fields)) {
            rs.add(new Field(field.getRootFieldName(), field.getIndex(), field.getKey(), field.getFieldType(), field.getField()));
        }
        String json = JsonUtils.writeToString(rs);
        System.out.println(json);

        org.joda.time.format.DateTimeFormatter dateTimeFrmt  = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-ddTHH:mm:ss").withZone(org.joda.time.DateTimeZone.UTC);
        DateTime dateTime = dateTimeFrmt.parseDateTime("20170924T104039");
        System.out.println(dateTime);
    }

    @Test
    public void testAddRootSpec() {
        String cols = "e\n" +
                "pu\n" +
                "page\n" +
                "pgrp\n" +
                "lnk\n" +
                "pglk\n" +
                "pgln\n" +
                "calc\n" +
                "dsid\n" +
                "gloc id\n" +
                "gccd\n" +
                "dvis\n" +
                "dvid\n" +
                "os\n" +
                "device_id\n" +
                "mdvs\n" +
                "mosv\n" +
                "mapv\n" +
                "apid\n" +
                "mcar\n" +
                "ch\n" +
                "srpg\n" +
                "rsta\n" +
                "tsrce\n" +
                "ip\n" +
                "Pros\n" +
                "bot\n" +
                "token\n" +
                "fltk\n" +
                "flow_logging_id\n" +
                "ru\n" +
                "trid\n" +
                "pfid\n" +
                "erpg\n" +
                "eccd\n" +
                "goal\n" +
                "fltp\n" +
                "amt\n" +
                "api_actor_id\n" +
                "api_integration_type\n" +
                "api_name\n" +
                "api_version\n" +
                "attempt_intention\n" +
                "ba_accepted\n" +
                "ba_created\n" +
                "ba_id\n" +
                "ba_purchase\n" +
                "billing_type\n" +
                "bml_reuse_status\n" +
                "bml_status\n" +
                "browser_type\n" +
                "btyp\n" +
                "buyer_id\n" +
                "buyer_ip\n" +
                "buyer_ip_country\n" +
                "buyer_type\n" +
                "cc_txn_id\n" +
                "channel\n" +
                "Chosen_funding_option\n" +
                "click_source\n" +
                "client_id\n" +
                "context_id\n" +
                "context_type\n" +
                "credit_type\n" +
                "currency_code\n" +
                "default_bank_name\n" +
                "default_funding_option\n" +
                "default_card_type\n" +
                "default_fulfillment_plan\n" +
                "design\n" +
                "ebay_txn_id\n" +
                "eligible_payment_methods\n" +
                "error_code\n" +
                "error_source\n" +
                "error_type\n" +
                "experience\n" +
                "external_error_code\n" +
                "feed_name\n" +
                "financing_code_selected\n" +
                "http_response_code\n" +
                "identity_access_token\n" +
                "integration_type\n" +
                "internal_error_code\n" +
                "is_autoredirect_enabled\n" +
                "is_bml_reuse\n" +
                "is_cookied_user\n" +
                "is_incentive_applied\n" +
                "is_ipn_enabled\n" +
                "is_multi_seller_payments\n" +
                "is_pdt_enabled\n" +
                "is_qiwi\n" +
                "is_qiwi_shown\n" +
                "is_remember_me\n" +
                "landing_page\n" +
                "market\n" +
                "merchant_cart_id\n" +
                "mobile_app_id\n" +
                "mobile_os\n" +
                "mozart_routing_reason\n" +
                "mozart_traffic_segment_name\n" +
                "oauth_grant_type\n" +
                "payment_service\n" +
                "planning_activity_handle\n" +
                "pmt_action\n" +
                "pmt_flow_id\n" +
                "pmt_status\n" +
                "redirect_reason\n" +
                "routing_reason\n" +
                "seller_id\n" +
                "solution_type\n" +
                "state_name\n" +
                "traffic_segment_name\n" +
                "transition_name\n" +
                "userselectedfundingsource\n" +
                "display_name\n" +
                "ads_intercept\n" +
                "ads_load_time\n" +
                "ads_success\n" +
                "apin\n" +
                "apiv\n" +
                "app_load_time\n" +
                "app_name\n" +
                "auth_complete_time\n" +
                "auth_req_status\n" +
                "auth_states\n" +
                "auth_success\n" +
                "authflowTriggered\n" +
                "biz_api_name\n" +
                "biz_api_operation\n" +
                "bizeventname\n" +
                "bver\n" +
                "cal_correlation_id\n" +
                "comp\n" +
                "error_description\n" +
                "keep_me_loggedin\n" +
                "kmpli_optin\n" +
                "oauth_client_id\n" +
                "oauth_client_name\n" +
                "registration_channel \n" +
                "remember_me_flag\n" +
                "risk_entries\n" +
                "srce\n" +
                "tgt_client_id\n" +
                "actor_acct_num\n" +
                "decr_transaction_id\n" +
                "docid\n" +
                "eligibility_answer\n" +
                "eligibility_reason\n" +
                "ext_error_code\n" +
                "fpti\n" +
                "obct\n" +
                "obex\n" +
                "obi\n" +
                "obic\n" +
                "obid\n" +
                "obitc\n" +
                "onboarding_business_channel\n" +
                "onboarding_experience\n" +
                "server_error_message\n" +
                "txn_id\n" +
                "encr_flow_token\n" +
                "vid\n" +
                "registration_channel\n" +
                "int_err_code\n" +
                "device_token\n" +
                "ref_tsrce\n" +
                "curr\n" +
                "ppgrp\n" +
                "tmpl\n" +
                "app_guid\n" +
                "unptid\n" +
                "cust_type\n" +
                "sw\n" +
                "sh\n" +
                "dh\n" +
                "dw\n" +
                "osv\n" +
                "dvmdl\n" +
                "gconn\n" +
                "guser\n" +
                "gisp\n" +
                "feed_name\n" +
                "ch\n" +
                "mobile_device_model\n" +
                "mobile_os\n" +
                "mobile_carrier";
        String metricStr = "dsid\n" +
                "device_id\n" +
                "ip\n" +
                "token\n" +
                "fltk\n" +
                "flow_logging_id\n" +
                "trid\n" +
                "pfid\n" +
                "api_actor_id\n" +
                "ba_id\n" +
                "buyer_id\n" +
                "buyer_ip\n" +
                "cc_txn_id\n" +
                "context_id\n" +
                "ebay_txn_id\n" +
                "merchant_cart_id\n" +
                "pmt_flow_id\n" +
                "seller_id\n" +
                "cal_correlation_id\n" +
                "decr_transaction_id\n" +
                "fpti\n" +
                "txn_id\n" +
                "encr_flow_token\n" +
                "vid\n" +
                "amt\n" +
                "billing_type";
        List<String> metrics = Lists.newArrayList(metricStr.split("\n"));
        List<Field> rs = new ArrayList<Field>();
        for (String column : cols.split("\n")) {
            rs.add(new Field("attributes", -1, new Utf8(column), FieldType.STRING, null));
        }
        String json = JsonUtils.writeToString(rs);
        System.out.println(json);
        List<String> dimensions = new ArrayList<String>();
        for (String column : cols.split("\n")) {
            if (!metrics.contains(column))
                dimensions.add(column);
        }
        json = JsonUtils.writeToString(dimensions);
        System.out.println("Dimensions : " + json);
        ArrayList<Metric> metricList = new ArrayList<>();
        List<String> excludedMetrics = Arrays.asList("amt", "billing_type");
        for (String metric : metrics) {
            if (!excludedMetrics.contains(metric))
                metricList.add(new Metric("hyperUnique", "dist_" + metric, metric));
        }
        json = JsonUtils.writeToString(metricList);
        System.out.println("Metrics : " + json);
    }

    private static class Field {
        private final String rootFieldName;
        private final int index;
        private final String key;
        private final FieldType fieldType;
        private final io.druid.data.input.parquet.model.Field field;

        private Field(String rootFieldName,
                      int index,
                      Utf8 key,
                      FieldType fieldType,
                      io.druid.data.input.parquet.model.Field field) {
            this.rootFieldName = rootFieldName;
            this.index = index;
            this.key = key.toString();
            this.fieldType = fieldType;
            this.field = field;
        }

        public String getRootFieldName() {
            return rootFieldName;
        }

        public int getIndex() {
            return index;
        }

        public String getKey() {
            return key;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public io.druid.data.input.parquet.model.Field getField() {
            return field;
        }
    }

    private static class Metric {
        private final String type;
        private final String name;
        private final String fieldName;

        public Metric(String type, String name, String fieldName) {
            this.type = type;
            this.name = name;
            this.fieldName = fieldName;
        }

        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public String getFieldName() {
            return fieldName;
        }

    }
}
