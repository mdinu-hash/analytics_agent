==================================== Overview ====================================
Create a modern, executive-focused chat interface for a Growth Analytics Agent that provides instant access to data insights through natural language queries. The interface should be deployable in databricks as a streamlit app (databricks compatible).

**FINAL WORKING IMPLEMENTATION** - This spec reflects the completed, tested design.

==================================== Layout Structure ====================================
**Single Panel Layout**: No sidebar - simplified design for all devices
**No Sidebar**: Removed complexity - "Clear & New Chat" button in main header area
**Responsive**: Works consistently across desktop, tablet, and mobile
**Background**: Purple gradient throughout (linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%))

==================================== Typography & Colors ====================================
Font: Maven Pro (Google Fonts) - weights 400, 500, 600, 700
Background: 3-color gradient linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)
Accent color: Purple #8b5cf6 (buttons, focus states, AI avatar)
Hover color: Darker purple #7c3aed
Text color: White #ffffff (headers), Black #111827 (welcome title)

==================================== Main Header ====================================
**Title**: "Growth Analytics Agent" (24px, 700 weight, #ffffff)
**Subtitle**: "Amazon Reviews (2002-2023)" (14px, rgba(255, 255, 255, 0.9))
**Layout**: Title and subtitle stacked vertically, left-aligned
**Divider Line**: Subtle line under header for visual separation
**Clear Button**: "🗑️ Clear & New Chat" button with spacing below header
**Background**: Inherits purple gradient

==================================== Welcome Screen (Initial State) ====================================
**Container**: Centered layout with maximum 600px width
**Title**: "Ask anything about your data" (32px desktop, 28px tablet, 20px mobile, 600 weight, #111827)
**Example Prompts**: 3 buttons stacked vertically, centered
**Button Style**: White background, #e5e7eb border, 12px border-radius
**Mobile Text**: 9px font size (much smaller than welcome title)
**Content**:
- "Why did adidas ratings decrease in early 2016?"
- "Which companies drove rating improvements since 2022?" 
- "How did ratings change over time per company?"

==================================== Chat Interface ====================================
**Message Layout**: User and AI messages with avatars
**User Messages**: Purple gradient background, "You" avatar (teal #0d9488)
**AI Messages**: Purple gradient background, "AI" avatar (purple #8b5cf6)
**Message Structure**: 32x32px rounded avatar + message content
**Max Width**: Constrained for readability
**Background**: All messages inherit purple gradient

==================================== Chat Input ====================================
**Technology**: st.chat_input() - native Streamlit component
**Position**: Bottom of page, after all messages
**Width**: Constrained using st.columns([1, 2, 1]) - matches example prompts width
**Callback**: on_submit=handle_chat_submit for immediate welcome screen transition
**Mobile Compatibility**: No containers/wrappers around st.chat_input for mobile visibility
**Placeholder**: "Ask anything about your data..."

==================================== State Management ====================================
**Welcome Screen Condition**: `if st.session_state.show_welcome and not st.session_state.messages:`
**Transition Logic**: 
- Chat input: Uses on_submit callback to set show_welcome=False immediately
- Example prompts: Adds message to messages array, natural transition via message check
**Welcome Screen Position**: Rendered AFTER prompt processing for proper transitions
**Key Variables**:
- st.session_state.show_welcome: Boolean flag
- st.session_state.messages: Array of chat messages
- st.session_state.pending_prompt: Bridge for chat input processing

==================================== Mobile Optimizations ====================================
**Responsive Breakpoints**:
- Desktop: Full layout
- Tablet (768px and below): Reduced font sizes
- Mobile (480px and below): Smallest fonts, single column layout

**Mobile Specifications**:
- Header title: 18px
- Dataset badge: 11px  
- Welcome title: 20px
- Example prompts: 9px (much smaller than welcome title)
- No sidebar (simplified UX)
- st.chat_input works natively without containers

==================================== Technical Implementation ====================================
**Framework**: Streamlit with st.chat_input()
**CSS Strategy**: Minimal CSS, rely on Streamlit native components
**Chat Input**: Direct in main body, no layout containers for mobile compatibility
**State Bridge**: Use st.session_state.pending_prompt for chat input processing
**Transition Timing**: Welcome screen check happens after all prompt processing
**Message Handling**: Single message addition point to avoid duplicates

==================================== Critical Success Factors ====================================
1. **st.chat_input() Mobile**: Must be in main body, no containers/columns wrapper
2. **Callback Timing**: on_submit callback provides immediate welcome screen transitions  
3. **Processing Order**: Welcome screen logic AFTER prompt processing for example prompt transitions
4. **State Management**: Use message count AND show_welcome flag for reliable transitions
5. **Width Constraint**: Use st.columns() to match input width to example prompts width
6. **Background Consistency**: All elements inherit purple gradient background

==================================== Deployment Notes ====================================
**Databricks Compatible**: Uses only standard Streamlit components
**Dependencies**: Streamlit, langchain components for LLM integration
**No Custom JS**: Pure Streamlit implementation for maximum compatibility
**Mobile Tested**: Works on mobile devices with proper st.chat_input() placement
**LLM Provider**: Supports both OpenAI and Anthropic via abstraction layer

==================================== Lesson Learned ====================================
**Key Insight**: st.chat_input() mobile visibility issues were caused by wrapping it in layout containers. The Streamlit docs specifically warn against this. Always use st.chat_input() directly in the main body for mobile compatibility.

**Transition Success**: The on_submit callback approach works perfectly for immediate UI state changes, while example prompts rely on natural message-based transitions.

**State Management**: Processing order matters - welcome screen logic must come after prompt processing to handle example prompt transitions properly.