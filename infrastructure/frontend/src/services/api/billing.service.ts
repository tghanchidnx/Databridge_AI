import { BaseApiService } from "./base.service";

export interface BillingPlan {
  id: string;
  organizationId: string;
  plan: "free" | "pro" | "enterprise";
  status: "active" | "cancelled" | "past_due";
  billingCycle: "monthly" | "annual";
  amount: number;
  currency: string;
  nextBillingDate?: string;
  createdAt: string;
  updatedAt: string;
}

export interface BillingHistory {
  id: string;
  organizationId: string;
  amount: number;
  currency: string;
  status: "paid" | "pending" | "failed";
  description: string;
  invoiceUrl?: string;
  paidAt?: string;
  createdAt: string;
}

export interface PaymentMethod {
  id: string;
  type: "card" | "bank_account";
  last4: string;
  brand?: string;
  expiryMonth?: number;
  expiryYear?: number;
  isDefault: boolean;
}

export class BillingService extends BaseApiService {
  /**
   * Get current billing plan
   */
  async getCurrentPlan(organizationId: string): Promise<BillingPlan> {
    const response = await this.api.get(
      `/organizations/${organizationId}/billing/plan`
    );
    return this.extractData(response);
  }

  /**
   * Get billing history
   */
  async getBillingHistory(organizationId: string): Promise<BillingHistory[]> {
    const response = await this.api.get(
      `/organizations/${organizationId}/billing/history`
    );
    return this.extractData(response);
  }

  /**
   * Update billing plan
   */
  async updatePlan(
    organizationId: string,
    plan: "free" | "pro" | "enterprise",
    billingCycle?: "monthly" | "annual"
  ): Promise<BillingPlan> {
    const response = await this.api.put(
      `/organizations/${organizationId}/billing/plan`,
      {
        plan,
        billingCycle,
      }
    );
    return this.extractData(response);
  }

  /**
   * Get payment methods
   */
  async getPaymentMethods(organizationId: string): Promise<PaymentMethod[]> {
    const response = await this.api.get(
      `/organizations/${organizationId}/billing/payment-methods`
    );
    return this.extractData(response);
  }

  /**
   * Add payment method
   */
  async addPaymentMethod(
    organizationId: string,
    paymentToken: string
  ): Promise<PaymentMethod> {
    const response = await this.api.post(
      `/organizations/${organizationId}/billing/payment-methods`,
      { paymentToken }
    );
    return this.extractData(response);
  }

  /**
   * Remove payment method
   */
  async removePaymentMethod(
    organizationId: string,
    paymentMethodId: string
  ): Promise<void> {
    await this.api.delete(
      `/organizations/${organizationId}/billing/payment-methods/${paymentMethodId}`
    );
  }

  /**
   * Set default payment method
   */
  async setDefaultPaymentMethod(
    organizationId: string,
    paymentMethodId: string
  ): Promise<PaymentMethod> {
    const response = await this.api.patch(
      `/organizations/${organizationId}/billing/payment-methods/${paymentMethodId}/default`
    );
    return this.extractData(response);
  }

  /**
   * Download invoice
   */
  async downloadInvoice(
    organizationId: string,
    invoiceId: string
  ): Promise<Blob> {
    const response = await this.api.get(
      `/organizations/${organizationId}/billing/invoices/${invoiceId}/download`,
      { responseType: "blob" }
    );
    return response.data;
  }
}

export const billingService = new BillingService();
