from odoo import models, api, fields, _
from odoo.tools.misc import format_date

from dateutil.relativedelta import relativedelta
from itertools import chain


class ReportAccountAgedPartnerCustom(models.AbstractModel):
    
    _inherit = "account.aged.partner"
    
    so_name = fields.Char(group_operator='max')
    invoice_date = fields.Char(group_operator='max')
    
    @api.model
    def _get_sql(self):
        options = self.env.context['report_options']
        query = ("""
            WITH last_rates AS (
                SELECT DISTINCT ON(rate.currency_id, rate.company_id)
                    rate.currency_id, rate.company_id, rate.rate
                FROM res_currency_rate rate
                WHERE rate.name <= %(date)s AND rate.rate > 0
                ORDER BY rate.currency_id, rate.company_id, rate.name DESC
            )
            SELECT
                {move_line_fields}, so.so_name as so_name, 
                account_move_line.partner_id AS partner_id,
                partner.name AS partner_name,
                COALESCE(trust_property.value_text, 'normal') AS partner_trust,
                COALESCE(account_move_line.currency_id, journal.currency_id) AS report_currency_id,
                account_move_line.payment_id AS payment_id,
                COALESCE(account_move_line.date_maturity, account_move_line.date) AS report_date,
                account_move_line.expected_pay_date AS expected_pay_date,
                move.move_type AS move_type,
                move.name AS move_name, move.invoice_date as invoice_date,
                journal.code AS journal_code,
                account.name AS account_name,
                account.code AS account_code,""" + ','.join([("""
                CASE WHEN period_table.period_index = {i}
                THEN %(sign)s *
                    CASE WHEN (
                        account_move_line.company_currency_id != account_move_line.currency_id
                        AND (bool_and(part_debit.debit_currency_id = account_move_line.currency_id) OR COUNT(part_debit) = 0)
                        AND (bool_and(part_credit.credit_currency_id = account_move_line.currency_id) OR COUNT(part_credit) = 0)
                    )
                    THEN ROUND((
                        account_move_line.amount_currency - COALESCE(SUM(part_debit.debit_amount_currency), 0) + COALESCE(SUM(part_credit.credit_amount_currency), 0)
                    ) * COALESCE(company_currency.rate, 1) / COALESCE(used_currency.rate, 1), currency_table.precision)
                    ELSE ROUND((
                        account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0)
                    ) * currency_table.rate, currency_table.precision) END
                ELSE 0 END AS period{i}""").format(i=i) for i in range(6)]) + """
            FROM account_move_line
            JOIN account_move move ON account_move_line.move_id = move.id
            LEFT JOIN (
                select   so.name as so_name , am.id as move_id from sale_order_line_invoice_rel as solir
                join sale_order_line as sol on solir.order_line_id = sol.id  
                join sale_order as so on sol.order_id = so.id 
                join account_move_line as aml on solir.invoice_line_id = aml.id
                join account_move as am on aml.move_id = am.id
                group by am.id, so.name
            )as so on move.id = so.move_id 
            JOIN account_journal journal ON journal.id = account_move_line.journal_id
            JOIN account_account account ON account.id = account_move_line.account_id
            JOIN res_partner partner ON partner.id = account_move_line.partner_id
            LEFT JOIN ir_property trust_property ON (
                trust_property.res_id = 'res.partner,'|| account_move_line.partner_id
                AND trust_property.name = 'trust'
                AND trust_property.company_id = account_move_line.company_id
            )
            JOIN {currency_table} ON currency_table.company_id = account_move_line.company_id
            LEFT JOIN LATERAL (
                SELECT part.amount, part.debit_move_id, part.debit_amount_currency, part.debit_currency_id
                FROM account_partial_reconcile part
                WHERE part.max_date <= %(date)s
            ) part_debit ON part_debit.debit_move_id = account_move_line.id
            LEFT JOIN LATERAL (
                SELECT part.amount, part.credit_move_id, part.credit_amount_currency, part.credit_currency_id
                FROM account_partial_reconcile part
                WHERE part.max_date <= %(date)s
            ) part_credit ON part_credit.credit_move_id = account_move_line.id
            LEFT JOIN last_rates AS company_currency ON company_currency.currency_id = account_move_line.company_currency_id
                AND company_currency.company_id = account_move_line.company_id
            LEFT JOIN last_rates AS used_currency ON used_currency.currency_id = account_move_line.currency_id
                AND used_currency.company_id = account_move_line.company_id
            JOIN {period_table} ON (
                period_table.date_start IS NULL
                OR COALESCE(account_move_line.date_maturity, account_move_line.date) <= DATE(period_table.date_start)
            )
            AND (
                period_table.date_stop IS NULL
                OR COALESCE(account_move_line.date_maturity, account_move_line.date) >= DATE(period_table.date_stop)
            )
            WHERE account.internal_type = %(account_type)s
            GROUP BY account_move_line.id, partner.id, trust_property.id, journal.id, move.id, account.id,
                     period_table.period_index, currency_table.rate, currency_table.precision, company_currency.rate, used_currency.rate, so.so_name
            HAVING CASE WHEN (
                account_move_line.company_currency_id != account_move_line.currency_id
                AND (bool_and(part_debit.debit_currency_id = account_move_line.currency_id) OR COUNT(part_debit) = 0)
                AND (bool_and(part_credit.credit_currency_id = account_move_line.currency_id) OR COUNT(part_credit) = 0)
            )
            THEN ROUND(account_move_line.amount_currency - COALESCE(SUM(part_debit.debit_amount_currency), 0) + COALESCE(SUM(part_credit.credit_amount_currency), 0), currency_table.precision) != 0
            ELSE ROUND(account_move_line.balance - COALESCE(SUM(part_debit.amount), 0) + COALESCE(SUM(part_credit.amount), 0), currency_table.precision) != 0 END
        """).format(
            move_line_fields=self._get_move_line_fields('account_move_line'),
            currency_table=self.env['res.currency']._get_query_currency_table(options),
            period_table=self._get_query_period_table(options),
        )
        params = {
            'account_type': options['filter_account_type'],
            'sign': 1 if options['filter_account_type'] == 'receivable' else -1,
            'date': options['date']['date_to'],
        }
        return self.env.cr.mogrify(query, params).decode(self.env.cr.connection.encoding)
    
    
    @api.model
    def _get_column_details(self, options):
        
        if options["filter_account_type"] == "receivable":
        
            return [
                self._header_column(),
                self._field_column('so_name'),
                self._field_column('invoice_date'),
                self._field_column('report_date'),
                self._field_column('journal_code', name="Journal"),
                self._field_column('account_name', name="Account"),
                self._field_column('expected_pay_date'),
                self._field_column('period0', name=_("As of: %s") % format_date(self.env, options['date']['date_to'])),
                self._field_column('period1', sortable=True),
                self._field_column('period2', sortable=True),
                self._field_column('period3', sortable=True),
                self._field_column('period4', sortable=True),
                self._field_column('period5', sortable=True),
                self._custom_column(  # Avoid doing twice the sub-select in the view
                    name=_('Total'),
                    classes=['number'],
                    formatter=self.format_value,
                    getter=(lambda v: v['period0'] + v['period1'] + v['period2'] + v['period3'] + v['period4'] + v['period5']),
                    sortable=True,
                ),
            ]
        else :
            return [
            self._header_column(),
            self._field_column('report_date'),
            self._field_column('journal_code', name="Journal"),
            self._field_column('account_name', name="Account"),
            self._field_column('expected_pay_date'),
            self._field_column('period0', name=_("As of: %s") % format_date(self.env, options['date']['date_to'])),
            self._field_column('period1', sortable=True),
            self._field_column('period2', sortable=True),
            self._field_column('period3', sortable=True),
            self._field_column('period4', sortable=True),
            self._field_column('period5', sortable=True),
            self._custom_column(  # Avoid doing twice the sub-select in the view
                name=_('Total'),
                classes=['number'],
                formatter=self.format_value,
                getter=(lambda v: v['period0'] + v['period1'] + v['period2'] + v['period3'] + v['period4'] + v['period5']),
                sortable=True,
            ),
        ]