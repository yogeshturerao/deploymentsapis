import json
import boto3
def lambda_handler(event, context):
    client = boto3.client('dynamodb')
    # TODO implement
    Cronjob = '''Staging,one-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE claims:geocode,claims:geocode
Staging,one-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env= COMPANYCODE assignment:run,assignment:run
Staging,one-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env= COMPANYCODE assignment:send_outbound_note_command,assignment:send_outbound_note_command
Staging,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:run_overflow,assignment:run_overflow
Staging,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console report_engine:update_last_assignment_note --env=COMPANYCODE,report_engine:update_last_assignment_note
Staging,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:send_outbound_note_command --env=COMPANYCODE,assignment:send_outbound_note_command
Staging,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:pdf_cron_stacking --env=COMPANYCODE,claim_management:pdf_cron_stacking
Staging,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:ack_letter_stacking --env=COMPANYCODE,claim_management:ack_letter_stacking
Staging,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:reports_cron_stacking --env=COMPANYCODE,claim_management:reports_cron_stacking
Staging,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:email_composer_send --env=COMPANYCODE,claim_management:email_composer_send
Staging,five-minute.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console core:open_claims_views --env=COMPANYCODE,core:open_claims_views
Staging,schedule_aa.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:schedule_aa,assignment:schedule_aa
Staging,hourly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console assignment:populate_work_days --env=COMPANYCODE,assignment:populate_work_days
Staging,hourly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console assignment:reset_weekly_limits --env=COMPANYCODE,assignment:reset_weekly_limits
Staging,hourly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console assignment:limits:cleanup --env=COMPANYCODE,assignment:limits:cleanup
Staging,hourly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console assignment:reset_weekly_limits --env=COMPANYCODE,assignment:reset_weekly_limits
Staging,hourly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console assignment:populate_work_days --env=COMPANYCODE,assignment:populate_work_days
Staging,weekly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console limits:populate_vendor_county_percentages --env=COMPANYCODE,limits:populate_vendor_county_percentages
Staging,weekly.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console limits:populate_vendor_percentages --env=COMPANYCODE,limits:populate_vendor_percentages
Production,one-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE claims:geocode,claims:geocode
Production,one-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:run,assignment:run
Production,one-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:send_outbound_note_command,assignment:send_outbound_note_command
Production,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:run_overflow --env=COMPANYCODE,assignment:run_overflow
Production,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console report_engine:update_last_assignment_note --env=COMPANYCODE,report_engine:update_last_assignment_note
Production,five-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:send_outbound_note_command --env=COMPANYCODE,assignment:send_outbound_note_command
Production,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:pdf_cron_stacking -- env=COMPANYCODE,claim_management:pdf_cron_stacking
Production,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:ack_letter_stacking -- env=COMPANYCODE,claim_management:ack_letter_stacking
Production,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:reports_cron_stacking -- env=COMPANYCODE,claim_management:reports_cron_stacking
Production,five-minute.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console claim_management:email_composer_send -- env=COMPANYCODE,claim_management:email_composer_send
Production,five-minute.sh,CARRIER,sudo -u www-codebase php $BASE_PATH/bin/console core:open_claims_views --env=COMPANYCODE,core:open_claims_views
Production,ten-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console mail:create_mailers --env=COMPANYCODE,mail:create_mailers
Production,ten-minute.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console mail:send_mailers --env=COMPANYCODE,mail:send_mailers
Production,hourly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:reset_weekly_limits -- env=COMPANYCODE,assignment:reset_weekly_limits
Production,hourly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:populate_work_days --env=COMPANYCODE,assignment:populate_work_days
Production,hourly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:limits:cleanup --env=COMPANYCODE,assignment:limits:cleanup
Production,hourly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:vendor_sync --env=COMPANYCODE,assignment:vendor_sync
Production,hourly.sh,ICM,sudo -u www-codebase php $BASE_PATH/bin/console cron:docusign --env=COMPANYCODE,cron:docusign
Production,weekly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console limits:populate_vendor_county_percentages --env=COMPANYCODE,limits:populate_vendor_county_percentages
Production,weekly.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console limits:populate_vendor_percentages --env=COMPANYCODE,limits:populate_vendor_percentages
Production,daily.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console assignment:populate_work_days -- env=COMPANYCODE,assignment:populate_work_days
Production,daily.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console resource:remove-duplicate-zip -- env=COMPANYCODE,resource:remove-duplicate-zip
Production,reconcile.sh,All,sudo -u www-codebase php /var/www/vhosts/codebase.claimaticapp.com/htdocs/bin/console claim_counts:reconcile --env=COMPANYCODE,claim_counts:reconcile
Production,run_aa.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE claims:geocode,claims:geocode
Production,run_aa.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:run,assignment:run
Production,run_aa.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:send_outbound_note_command,assignment:send_outbound_note_command
Production,schedule_aa.sh,All,sudo -u www-codebase php $BASE_PATH/bin/console --env=COMPANYCODE assignment:schedule_aa,assignment:schedule_aa
'''
    for line in Cronjob.splitlines():
        deployment,filename,customertype,command,aliasname = line.split(',')
        response1 = client.transact_write_items(
            TransactItems= [
                {
                    'Put': {
                        'TableName': 'CronJobListMaster',
                        'Item': {
                            'Deployment': { 'S': deployment },
                            'FileCommandAlias': { 'S': "_".join([filename, aliasname]) },
                            'FileName': { 'S': filename },
                            'CustomerType': { 'S': customertype },
                            'Command': { 'S': command },
                            'AliasName': { 'S': aliasname }
                            }  
                        }
                    }
                    ])
