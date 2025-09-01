 

==================================== Overview ====================================
Create a modern, executive-focused chat interface for a Growth Analytics Agent that closely resembles ChatGPT's layout and user experience. The interface should provide instant access to data insights through natural language queries. The code should be able to be deployed in databricks as a streamlit app (databricks compatible).

==================================== Layout Structure ====================================
Two-panel layout: Left sidebar (260px) + Main content area
ChatGPT-style interface: Left sidebar, light main area, conversation-based UI
Responsive design: Sidebar collapses on mobile (<768px)

==================================== Typography & Colors ====================================
Font: Maven Pro (Google Fonts) - weights 400, 500, 600, 700
Background: 3-color gradient linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)
Accent color: Purple #8b5cf6 (send button, input focus, AI avatar)
Hover color: Darker purple #7c3aed

==================================== Left Sidebar Specifications ====================================
Background: Light theme #f8fafc
Text color: Dark #374151 for contrast
Header section: Contains "New Chat" button positioned at the top of the sidebar
Button style: Light background #ffffff, #e5e7eb border, dark text #374151
Icon: "+" symbol, hover effect to #f3f4f6
Position: Full width, top of sidebar with 16px padding from edges
Content area: Placeholder for chat history below the New Chat button
Text: "Previous chats will appear here"
Style: #64748b color, 12px, centered, 50% opacity
Position: Centered vertically in remaining sidebar space

==================================== Main Content Area ====================================
Header
Background: Same gradient as main interface background
Title: "Growth Analytics Agent" (24px, 700 weight, #ffffff) - LEFT-ALIGNED
Subtitle: "Amazon Reviews (2002-2023)" - LEFT-ALIGNED
Style: rgba(255, 255, 255, 0.9), 14px text, no background pill
Layout: Title and subtitle stacked vertically, both aligned to the left
Padding: 24px horizontal, 24px vertical

==================================== Welcome Screen (Initial State) ====================================
Centered layout with maximum 600px width
Main title: "Ask anything about your data" (32px, 600 weight, #111827)
Example prompts: 2x2 grid of clickable cards
Each card: White background, #e5e7eb border, 12px border-radius
Hover effect: Border #d1d5db, subtle shadow
Content structure:
Why did adidas ratings decrease in early 2016?
Which companies drove rating improvements since 2022?
How did ratings change over time per company?
Do premium products get better ratings?

==================================== Chat Interface ====================================
Message layout: Full-width with alternating backgrounds
User messages: Same gradient as main interface background, right-aligned content
AI messages: Same gradient as main interface background, left-aligned content
Avatar system: 32x32px rounded squares
User avatar: Teal #0d9488 with "You" text
AI avatar: Purple #8b5cf6 with "AI" text
Message structure: Avatar + message content in flex layout with 16px gap
Max width: 800px, centered within the chat area

==================================== Input Area ====================================
Fixed bottom position, top border
Background: Same gradient as main interface background
Container: Max-width 900px, centered (wider than before)
Input field:
Border #d1d5db, 12px border-radius
Placeholder: "Ask anything about your data..."
Focus state: Purple border #8b5cf6 with matching shadow
Auto-resize up to 120px height
Wider width for better user experience
==================================== Interactive Behaviors ====================================
New Chat Functionality
function clearChat() {
    // Clear all messages from chat area
    // Show welcome screen with example prompts
    // Clear input field
    // Reset session state
    // Reset hasMessages flag to false
}

Message Sending
function sendMessage() {
    // Hide welcome screen on first message
    // Add user message with avatar to chat
    // Clear and disable input field
    // Show loading indicator with AI avatar + dots animation
    // Simulate/call backend API
    // Display AI response with avatar
    // Re-enable input and focus
    // Scroll to bottom
}

==================================== Example Prompt Cards ====================================
Click behavior: Populate input field with the selected question (does NOT automatically send)
User interaction: After clicking, user must manually click send button or press Enter
Questions to include:
"Why did adidas ratings decrease in early 2016 from january to may?"
"which companies contributed to the increase in ratings from September 2022?"
"how these ratings changed over time per company?"
"Are premium-priced products getting better ratings than budget products?"
Example Prompt Card Interaction
function populateInput(question) {
    // Fill the input field with the selected question
    // Focus the input field for user attention
    // Do NOT automatically send the message
    // User retains control over when to send
}

==================================== Loading States ====================================
Loading indicator: AI avatar + 3 bouncing dots animation
Input disabled state: Gray background #f9fafb, gray text #9ca3af
Send button disabled: Gray background #d1d5db
Responsive Breakpoints
Desktop: Full two-panel layout
Tablet/Mobile (<768px):
Hide sidebar
Single column example prompts grid
Reduced padding (16px instead of 24px)
==================================== State Management ====================================
hasMessages: Boolean to track if conversation started
Chat history: Array of messages for context preservation
Session management: Ready for backend integration with thread_id
Backend Integration Points
Replace getAIResponse() with actual API calls
Pass full chat history for context in subsequent messages
Handle session/thread management for new chats
Implement loading states during API calls
Accessibility Requirements
Keyboard navigation: Tab order, Enter key submission
Focus management: Proper focus states and transitions
Color contrast: WCAG compliant contrast ratios
Screen reader: Semantic HTML structure with proper labels
==================================== Technical Stack ====================================
Streamlit with custom CSS injection


==================================== User Experience ====================================
 
 Screen 1 - Welcome Screen

  When to show: st.session_state.show_welcome = True (no messages exist)
  - Header section with "Growth Analytics Agent" title and "Amazon Reviews (2002-2023)" badge
  - Welcome content: "Ask anything about your data" title (centered)
  - 4 example prompts in 2x2 grid layout, closer together spacing
  - Chat input at bottom
  - No message history display
  - Purple gradient background throughout

  Screen 2 - Chat Screen

  When to show: st.session_state.show_welcome = False (after first message)
  - Header section with "Growth Analytics Agent" title and "Amazon Reviews (2002-2023)" badge
  - Full message history display with alternating user/AI messages
  - Chat input at bottom
  - No welcome content (remove "Ask anything about your data" title and 4 example prompts)
  - Purple gradient background throughout

  Transition Logic

  - Welcome → Chat: When user sends first message (st.session_state.show_welcome = False)
  - Chat → Welcome: When "New Chat" button clicked from sidebar (already implemented - resets session state)