// Export all API services
export * from "./base.service";
export * from "./auth.service";
export * from "./users.service";
export * from "./organizations.service";
export * from "./billing.service";
export * from "./templates.service";

// Export organized service modules
export * from "./connection";
export * from "./hierarchy";

// Re-export service instances
export { authService } from "./auth.service";
export { usersService } from "./users.service";
export { organizationsService } from "./organizations.service";
export { billingService } from "./billing.service";
export { templatesService } from "./templates.service";
export { connectionService } from "./connection";
export { projectService, smartHierarchyService } from "./hierarchy";

// Legacy compatibility - export old apiService structure
import { authService } from "./auth.service";
import { usersService } from "./users.service";
import { organizationsService } from "./organizations.service";
import { billingService } from "./billing.service";

export const apiService = {
  // Auth methods
  loginWithMicrosoft: authService.loginWithMicrosoft.bind(authService),
  loginWithEmail: authService.loginWithEmail.bind(authService),
  signup: authService.signup.bind(authService),
  logout: authService.logout.bind(authService),
  verifyToken: authService.verifyToken.bind(authService),

  // User methods
  getCurrentUser: usersService.getCurrentUser.bind(usersService),
  getUser: usersService.getUser.bind(usersService),
  updateUser: usersService.updateUser.bind(usersService),
  deleteUser: usersService.deleteUser.bind(usersService),
  changePassword: usersService.changePassword.bind(usersService),

  // Organization methods
  createOrganization:
    organizationsService.createOrganization.bind(organizationsService),
  getUserOrganizations:
    organizationsService.getUserOrganizations.bind(organizationsService),
  getOrganization:
    organizationsService.getOrganization.bind(organizationsService),
  updateOrganization:
    organizationsService.updateOrganization.bind(organizationsService),
  deleteOrganization:
    organizationsService.deleteOrganization.bind(organizationsService),
  getOrganizationMembers:
    organizationsService.getOrganizationMembers.bind(organizationsService),
  addMember: organizationsService.addMember.bind(organizationsService),
  removeMember: organizationsService.removeMember.bind(organizationsService),
  updateMemberRole:
    organizationsService.updateMemberRole.bind(organizationsService),

  // Billing methods
  getCurrentPlan: billingService.getCurrentPlan.bind(billingService),
  getBillingHistory: billingService.getBillingHistory.bind(billingService),
  updatePlan: billingService.updatePlan.bind(billingService),
  getPaymentMethods: billingService.getPaymentMethods.bind(billingService),
  addPaymentMethod: billingService.addPaymentMethod.bind(billingService),
  removePaymentMethod: billingService.removePaymentMethod.bind(billingService),
  setDefaultPaymentMethod:
    billingService.setDefaultPaymentMethod.bind(billingService),
  downloadInvoice: billingService.downloadInvoice.bind(billingService),
};
